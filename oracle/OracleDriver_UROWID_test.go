/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package oracle

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
	"github.com/oracle/go-oracledb/v26/oracle/utils"
)

// Project 105286: functional coverage of the Go driver's UROWID handling.
// Functional tests are numbered consecutively to match the local specification.
// All fixtures use ORACLE_LOADER, even when the DIRECTORY is DATA_PUMP_DIR.
//
// Run the basic native-fetch test with the existing private connection config:
//
//	go test -count=1 -v ./oracle -run '^TestDriver_UROWID_001_BasicFetch$' \
//	  -args -driver.config.filename=/absolute/path/to/private-config.json \
//	  -driver.config.name=urowid_local
//
// Run the full category through TestCategoryExecutor with -test.category=urowid.
// The suite does not create accounts, DIRECTORY objects, grants or DB settings.
var urowidDirectory = flag.String("test.urowid.directory", "DATA_PUMP_DIR", "existing Oracle DIRECTORY with READ/WRITE access")

// This supplied server file is required only for the maximum-length subtest.
// It must contain row 2,beta and produce a genuine 3950-internal-byte locator.
// The test reads this file but never owns, overwrites or deletes it.
var urowidBoundaryFile = flag.String("test.urowid.boundary.file", "", "confirmed server CSV filename producing a 3950-internal-byte UROWID; must contain id 2, payload beta")

const urowidMinimumCompatible = "23.27.1.0.0"
const urowidBaseCSV = "1,alpha\n2,beta\n3,gamma\n4,delta\n"
const urowidMaxRawLength = 3950
const urowidAccessParameters = `
    ACCESS PARAMETERS (
      RECORDS DELIMITED BY NEWLINE
      NOLOGFILE NOBADFILE NODISCARDFILE
      FIELDS TERMINATED BY ','
      MISSING FIELD VALUES ARE NULL
      (id INTEGER EXTERNAL, payload CHAR(40))
    )`

var urowidIdentifier = regexp.MustCompile(`^[A-Z][A-Z0-9_$#]{0,127}$`)
var urowidDumpHeader = regexp.MustCompile(`^Typ=208 Len=([0-9]+):\s*([0-9]+)`)
var urowidErrorCode = regexp.MustCompile(`(?:ORA|KUP|OGD)-[0-9]{5}`)
var urowidUnexpectedRow = errors.New("negative UROWID lookup returned a row")

// urowidFixture owns one reserved session and the tables/files created by a test.
// Ownership is recorded before mutations so cleanup also handles partial setup.
type urowidFixture struct {
	db       *sql.DB
	conn     *sql.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	dir      string
	prefix   string
	legacy   string
	extended string
	tables   []string
	files    []string
}

// urowidRow keeps native locator text alongside its expected application data.
type urowidRow struct {
	rid       string
	id        int64
	payload   string
	reference string // Same-query text used by projections, especially legacy values.
}

// Functional tests: native fetch, inputs, outputs and application round trips.

// TestDriver_UROWID_001_BasicFetch checks that one native UROWID can be fetched into a Go string.
func TestDriver_UROWID_001_BasicFetch(t *testing.T) {
	f := newUROWIDSession(t)
	file := f.prefix + "_basic.csv"
	f.writeFile(t, file, "1,alpha\n")
	table := f.createExternal(t, "BASIC", file, true)

	// Fetch the locator directly; a CAST would bypass native UROWID decoding.
	var rid string
	err := f.conn.QueryRowContext(f.ctx, "SELECT ROWID FROM "+table+" WHERE id = 1").Scan(&rid)
	urowidRequireNoError(t, "fetch one native UROWID", err)
	if rid == "" {
		t.Fatal("native UROWID must not be empty")
	}
}

// TestDriver_UROWID_002_NativeExtended checks native text and metadata, then consumes larger
// results completely.
func TestDriver_UROWID_002_NativeExtended(t *testing.T) {
	f := newUROWIDFixture(t)
	t.Run("four_rows_and_metadata", func(t *testing.T) {
		f.checkProjection(t, f.extended, true)
	})
	t.Run("larger_result_stream", func(t *testing.T) {
		f.openFreshSession(t)
		var csv strings.Builder
		for id := 1; id <= 257; id++ {
			fmt.Fprintf(&csv, "%d,row-%03d\n", id, id)
		}
		file := f.prefix + "_stream.csv"
		f.writeFile(t, file, csv.String())
		table := f.createExternal(t, "STREAM", file, true)
		// There is no public fetch-array-size setting in this Go driver.
		// Repeated complete scans exercise ordinary result-stream consumption.
		for pass := 1; pass <= 3; pass++ {
			rows, err := f.conn.QueryContext(f.ctx,
				"SELECT ROWID, CAST(ROWID AS VARCHAR2(4000)), id, RTRIM(payload) FROM "+table+" ORDER BY id")
			urowidRequireNoError(t, "query larger result stream", err)
			defer rows.Close()
			count, mismatches := 0, 0
			for rows.Next() {
				var native, reference, payload string
				var id int64
				if err := rows.Scan(&native, &reference, &id, &payload); err != nil {
					_ = rows.Close()
					urowidRequireNoError(t, "scan larger result stream", err)
				}
				count++
				if id != int64(count) || payload != fmt.Sprintf("row-%03d", count) || native != reference {
					mismatches++
				}
			}
			err = rows.Err()
			closeErr := rows.Close()
			urowidRequireNoError(t, "fetch larger result stream", err)
			urowidRequireNoError(t, "close larger result stream", closeErr)
			if count != 257 || mismatches != 0 {
				t.Errorf("scan %d: got %d/257 rows and %d identity/text mismatches", pass, count, mismatches)
			}
		}
	})
}

// TestDriver_UROWID_003_NativeLegacy checks native legacy locator text against the same returned
// row.
func TestDriver_UROWID_003_NativeLegacy(t *testing.T) {
	f := newUROWIDFixture(t)
	f.checkProjection(t, f.legacy, false)
}

// TestDriver_UROWID_004_PositionalAndLiteral checks native locator input as a positional bind
// and quoted literal.
func TestDriver_UROWID_004_PositionalAndLiteral(t *testing.T) {
	f := newUROWIDFixture(t)
	reference := f.referenceText(t, f.extended, 2)
	t.Run("positional", func(t *testing.T) {
		row := f.fetchNativeRow(t, f.extended, 2)
		urowidCheckText(t, row.rid, reference)
		f.checkQueryRows(t, "SELECT id, RTRIM(payload) FROM "+f.extended+" WHERE ROWID = :1",
			[]int64{2}, row.rid)
	})
	t.Run("quoted_literal", func(t *testing.T) {
		f.openFreshSession(t)
		row := f.fetchNativeRow(t, f.extended, 2)
		urowidCheckText(t, row.rid, reference)
		// Quote the complete value as SQL text; never interpolate it unescaped.
		f.checkQueryRows(t, "SELECT id, RTRIM(payload) FROM "+f.extended+" WHERE ROWID = "+utils.EnquoteLiteral(row.rid), []int64{2})
	})
	t.Run("database_reference_control", func(t *testing.T) {
		f.openFreshSession(t)
		f.checkLookup(t, f.extended, reference, 2)
	})
}

// TestDriver_UROWID_005_NamedBind checks that a native locator supplied as a named bind selects
// the right row.
func TestDriver_UROWID_005_NamedBind(t *testing.T) {
	f := newUROWIDFixture(t)
	row := f.fetchNativeRow(t, f.extended, 4)
	urowidCheckText(t, row.rid, f.referenceText(t, f.extended, 4))
	f.checkLookup(t, f.extended, row.rid, 4)
}

// TestDriver_UROWID_006_PreparedReuse checks that one prepared statement receives new locator
// values on every execution.
func TestDriver_UROWID_006_PreparedReuse(t *testing.T) {
	f := newUROWIDFixture(t)
	row2 := f.fetchNativeRow(t, f.extended, 2)
	row4 := f.fetchNativeRow(t, f.extended, 4)
	urowidCheckText(t, row2.rid, f.referenceText(t, f.extended, 2))
	urowidCheckText(t, row4.rid, f.referenceText(t, f.extended, 4))
	stmt, err := f.conn.PrepareContext(f.ctx,
		"SELECT id, RTRIM(payload) FROM "+f.extended+" WHERE ROWID = :rid")
	urowidRequireNoError(t, "prepare UROWID lookup", err)
	defer stmt.Close()
	// Returning to row 2 catches a stale bind left over from the previous call.
	for index, row := range []urowidRow{row2, row4, row2} {
		t.Run(fmt.Sprintf("execution_%d", index+1), func(t *testing.T) {
			rows, err := stmt.QueryContext(f.ctx, sql.Named("rid", row.rid))
			urowidRequireNoError(t, "execute reused UROWID lookup", err)
			urowidCheckRows(t, rows, []int64{row.id})
		})
	}
	urowidRequireNoError(t, "close prepared UROWID lookup", stmt.Close())
}

// TestDriver_UROWID_007_SavedColumnRoundTrip checks native UROWID-column readback and subsequent
// input binding.
func TestDriver_UROWID_007_SavedColumnRoundTrip(t *testing.T) {
	f := newUROWIDFixture(t)
	saved := f.createSavedTable(t)
	for _, id := range []int64{2, 4} {
		var rid string
		err := f.conn.QueryRowContext(f.ctx, "SELECT rid FROM "+saved+" WHERE id = :id",
			sql.Named("id", id)).Scan(&rid)
		urowidRequireNoError(t, "native saved UROWID fetch", err)
		urowidCheckText(t, rid, f.referenceText(t, f.extended, id))
		f.checkLookup(t, f.extended, rid, id)
	}
}

// TestDriver_UROWID_008_PLSQLOut checks complete OUT values, bind-back and recovery after an
// expected conversion error.
func TestDriver_UROWID_008_PLSQLOut(t *testing.T) {
	f := newUROWIDFixture(t)
	reference := f.referenceText(t, f.extended, 2)
	t.Run("native_comparison", func(t *testing.T) {
		row := f.fetchNativeRow(t, f.extended, 2)
		urowidCheckText(t, row.rid, reference)
	})
	blocks := []struct{ name, sql string }{
		{"local_urowid_to_out", "DECLARE locator UROWID; BEGIN SELECT ROWID INTO locator FROM " + f.extended + " WHERE id = 2; :rid := locator; END;"},
		{"direct_select_into_out", "BEGIN SELECT ROWID INTO :rid FROM " + f.extended + " WHERE id = 2; END;"},
	}
	for _, block := range blocks {
		t.Run(block.name, func(t *testing.T) {
			f.openFreshSession(t)
			var rid string
			f.execSQL(t, "PL/SQL UROWID OUT", block.sql, sql.Named("rid", sql.Out{Dest: &rid}))
			urowidCheckText(t, rid, reference)
			f.checkLookup(t, f.extended, rid, 2)
		})
	}
	t.Run("server_conversion_overflow", func(t *testing.T) {
		f.openFreshSession(t)
		// sql.Out has no MaxSize. This tests server error propagation, not a
		// Node/OCI-style undersized client output buffer.
		var rid string
		_, err := f.conn.ExecContext(f.ctx, `
			DECLARE locator UROWID; too_small VARCHAR2(17);
			BEGIN SELECT ROWID INTO locator FROM `+f.extended+` WHERE id = 2;
			  too_small := locator; :rid := too_small;
			END;`, sql.Named("rid", sql.Out{Dest: &rid}))
		urowidExpectError(t, err, "ORA-06502")
		// An expected error must leave this same session usable for UROWID work.
		f.execSQL(t, "OUT after conversion error", blocks[0].sql, sql.Named("rid", sql.Out{Dest: &rid}))
		urowidCheckText(t, rid, reference)
		f.checkLookup(t, f.extended, rid, 2)
	})
}

// TestDriver_UROWID_009_PLSQLInOut checks a native locator as input and a different complete
// locator as output.
func TestDriver_UROWID_009_PLSQLInOut(t *testing.T) {
	f := newUROWIDFixture(t)
	for _, name := range []string{"native_input", "database_reference_control"} {
		t.Run(name, func(t *testing.T) {
			f.openFreshSession(t)
			rid := f.referenceText(t, f.extended, 4)
			if name == "native_input" {
				row := f.fetchNativeRow(t, f.extended, 4)
				urowidCheckText(t, row.rid, rid)
				rid = row.rid
			}
			var id int64
			var payload string
			f.execSQL(t, "PL/SQL UROWID IN OUT", `
				DECLARE locator UROWID := :rid;
				BEGIN
				  SELECT id, RTRIM(payload) INTO :id, :payload FROM `+f.extended+` WHERE ROWID = locator;
				  SELECT ROWID INTO locator FROM `+f.extended+` WHERE id = 2;
				  :rid := locator;
				END;`, sql.Named("rid", sql.Out{Dest: &rid, In: true}),
				sql.Named("id", sql.Out{Dest: &id}), sql.Named("payload", sql.Out{Dest: &payload}))
			urowidCheckIdentity(t, id, payload, 4)
			urowidCheckText(t, rid, f.referenceText(t, f.extended, 2))
			f.checkLookup(t, f.extended, rid, 2)
		})
	}
}

// TestDriver_UROWID_010_RetainedValueSameSession checks that later fetches do not overwrite a
// retained Go locator string.
func TestDriver_UROWID_010_RetainedValueSameSession(t *testing.T) {
	f := newUROWIDFixture(t)
	retained := f.fetchNativeRow(t, f.extended, 2)
	snapshot := strings.Clone(f.referenceText(t, f.extended, 2))
	urowidCheckText(t, retained.rid, snapshot)
	for pass := 1; pass <= 3; pass++ {
		// Read other rows to exercise reuse of the driver's receive buffers.
		f.checkProjection(t, f.extended, true)
		urowidCheckText(t, retained.rid, snapshot)
		f.checkLookup(t, f.extended, retained.rid, 2)
	}
}

// TestDriver_UROWID_011_RetainedValueFreshSession checks that application-held text remains
// usable after its original pool closes.
func TestDriver_UROWID_011_RetainedValueFreshSession(t *testing.T) {
	f := newUROWIDFixture(t)
	retained := f.fetchNativeRow(t, f.extended, 2)
	snapshot := strings.Clone(f.referenceText(t, f.extended, 2))
	urowidCheckText(t, retained.rid, snapshot)
	var firstSID, secondSID string
	urowidRequireNoError(t, "read first session identity", f.conn.QueryRowContext(f.ctx,
		"SELECT DBMS_SESSION.UNIQUE_SESSION_ID FROM dual").Scan(&firstSID))
	f.openFreshSession(t)
	urowidRequireNoError(t, "read second session identity", f.conn.QueryRowContext(f.ctx,
		"SELECT DBMS_SESSION.UNIQUE_SESSION_ID FROM dual").Scan(&secondSID))
	if firstSID == secondSID {
		t.Fatal("PREREQUISITE: a different physical session is required")
	}
	// Verify the stored Go value, rather than comparing regenerated DB locators.
	other := f.fetchNativeRow(t, f.extended, 4)
	urowidCheckText(t, other.rid, f.referenceText(t, f.extended, 4))
	urowidCheckText(t, retained.rid, snapshot)
	f.checkLookup(t, f.extended, retained.rid, 2)
}

// TestDriver_UROWID_012_MultipleInputBinds checks that multiple native locator arguments reach
// the server intact.
func TestDriver_UROWID_012_MultipleInputBinds(t *testing.T) {
	f := newUROWIDFixture(t)
	row2 := f.fetchNativeRow(t, f.extended, 2)
	row4 := f.fetchNativeRow(t, f.extended, 4)
	urowidCheckText(t, row2.rid, f.referenceText(t, f.extended, 2))
	urowidCheckText(t, row4.rid, f.referenceText(t, f.extended, 4))
	f.checkQueryRows(t, "SELECT id, RTRIM(payload) FROM "+f.extended+" WHERE ROWID IN (:1,:2) ORDER BY id",
		[]int64{2, 4}, row4.rid, row2.rid)
}

// TestDriver_UROWID_013_VariableLengthAndBoundary checks complete native text and bind-back for
// genuine length fixtures.
func TestDriver_UROWID_013_VariableLengthAndBoundary(t *testing.T) {
	f := newUROWIDFixture(t)
	t.Run("genuine_variable_length", func(t *testing.T) {
		baseLength := f.fixtureLocatorLength(t, f.extended, 2, 6)
		file := f.prefix + "_" + urowidRandom(t, 96) + ".csv"
		f.writeFile(t, file, urowidBaseCSV)
		table := f.createExternal(t, "LONG", file, true)
		length := f.fixtureLocatorLength(t, table, 2, 6)
		if length == baseLength {
			t.Fatal("PREREQUISITE: variable-length fixtures must produce different internal locator lengths")
		}
		t.Logf("observed genuine internal lengths %d and %d", baseLength, length)
		row := f.fetchNativeRow(t, table, 2)
		urowidCheckText(t, row.rid, f.referenceText(t, table, 2))
		f.checkLookup(t, table, row.rid, 2)
	})
	t.Run("genuine_maximum_boundary", func(t *testing.T) {
		if *urowidBoundaryFile == "" {
			t.Fatalf("VALIDATION PREREQUISITE: supply a confirmed server LOADER fixture with -test.urowid.boundary.file that generates %d internal UROWID bytes; CSV size or a long filename alone does not prove the boundary", urowidMaxRawLength)
		}
		if strings.ContainsAny(*urowidBoundaryFile, "/\\") {
			t.Fatal("PREREQUISITE: boundary file must be a filename in the selected DIRECTORY")
		}
		f.openFreshSession(t)
		// The supplied file is read-only and never registered for deletion.
		table := f.createExternal(t, "BOUNDARY", *urowidBoundaryFile, true)
		length := f.fixtureLocatorLength(t, table, 2, 6)
		if length != urowidMaxRawLength {
			t.Fatalf("PREREQUISITE: supplied boundary fixture produces %d internal bytes, require %d", length, urowidMaxRawLength)
		}
		row := f.fetchNativeRow(t, table, 2)
		urowidCheckText(t, row.rid, f.referenceText(t, table, 2))
		f.checkLookup(t, table, row.rid, 2)
	})
}

// TestDriver_UROWID_014_MalformedAndNull checks informative input errors, SQL NULL and
// same-session UROWID recovery.
func TestDriver_UROWID_014_MalformedAndNull(t *testing.T) {
	f := newUROWIDFixture(t)
	reference := f.referenceText(t, f.extended, 2)
	f.checkLookup(t, f.extended, reference, 2)
	for _, input := range []struct{ name, rid string }{
		{"invalid_text", "#INVALID_UROWID#"},
		{"invalid_encoding", reference[:len(reference)/2] + "#" + reference[len(reference)/2+1:]},
	} {
		t.Run(input.name, func(t *testing.T) {
			err := f.lookupError(t, "SELECT id FROM "+f.extended+" WHERE ROWID = :rid", sql.Named("rid", input.rid))
			urowidExpectError(t, err, "ORA-01410")
			// Do not reconnect: the driver's error path must preserve this session.
			row := f.fetchNativeRow(t, f.extended, 2)
			urowidCheckText(t, row.rid, reference)
			f.checkLookup(t, f.extended, row.rid, 2)
		})
	}
	t.Run("sql_null", func(t *testing.T) {
		err := f.lookupError(t, "SELECT id FROM "+f.extended+" WHERE ROWID = :rid", sql.Named("rid", nil))
		if !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("NULL locator: expected NO_ROWS, got %s", urowidErrorSummary(err))
		}
	})
}

// TestDriver_UROWID_015_ConvertedTextRoundTrip checks server-converted locator strings and
// their reuse as application inputs. This standalone proof of concept runs directly by name;
// it does not assert native UROWID support or belong to the published test specification.
func TestDriver_UROWID_015_ConvertedTextRoundTrip(t *testing.T) {
	f := newUROWIDSession(t)
	file := f.prefix + "_text.csv"
	f.writeFile(t, file, urowidBaseCSV)
	table := f.createExternal(t, "TEXT", file, true)
	locators := make(map[int64]string)

	// Fetch character data only, then compare its complete value with the PL/SQL reference.
	if !t.Run("converted_fetch", func(t *testing.T) {
		for _, id := range []int64{2, 4} {
			var rid string
			var textLength int64
			err := f.conn.QueryRowContext(f.ctx,
				"SELECT CAST(ROWID AS VARCHAR2(4000)), LENGTH(CAST(ROWID AS VARCHAR2(4000))) FROM "+table+" WHERE id = :id",
				sql.Named("id", id)).Scan(&rid, &textLength)
			urowidRequireNoError(t, "fetch converted UROWID text", err)
			// Locator text is ASCII, so its character count must equal the Go byte length.
			if rid == "" || int64(len(rid)) != textLength {
				t.Fatalf("row %d: converted locator has %d bytes, server reports %d characters; require nonempty complete text",
					id, len(rid), textLength)
			}
			urowidCheckText(t, rid, f.referenceText(t, table, id))
			locators[id] = rid
		}
	}) {
		// The bind scenarios require successfully fetched and validated locator strings.
		return
	}

	// A positional string argument must retrieve exactly its original row and payload.
	t.Run("positional_bind", func(t *testing.T) {
		for _, id := range []int64{2, 4} {
			f.checkQueryRows(t, "SELECT id, RTRIM(payload) FROM "+table+" WHERE ROWID = :1",
				[]int64{id}, locators[id])
		}
	})

	// Exercise the same round trip through the public named-bind interface.
	t.Run("named_bind", func(t *testing.T) {
		for _, id := range []int64{2, 4} {
			f.checkLookup(t, table, locators[id], id)
		}
	})

	// Reuse one prepared statement for rows 2, 4 and 2 to catch stale bind values.
	t.Run("prepared_reuse", func(t *testing.T) {
		stmt, err := f.conn.PrepareContext(f.ctx,
			"SELECT id, RTRIM(payload) FROM "+table+" WHERE ROWID = :rid")
		urowidRequireNoError(t, "prepare converted UROWID lookup", err)
		defer stmt.Close()
		for _, id := range []int64{2, 4, 2} {
			rows, err := stmt.QueryContext(f.ctx, sql.Named("rid", locators[id]))
			urowidRequireNoError(t, "execute prepared converted UROWID lookup", err)
			urowidCheckRows(t, rows, []int64{id})
		}
		urowidRequireNoError(t, "close prepared converted UROWID lookup", stmt.Close())
	})
}

// Compatibility prerequisite: read the live parameter before creating resources.

// TestUROWIDCompatibleVersion checks numeric ordering and rejects malformed live parameter
// values without a database.
func TestUROWIDCompatibleVersion(t *testing.T) {
	cases := []struct {
		name      string
		value     string
		supported bool
		invalid   bool
	}{
		{"below_minor", "23.9.0.0.0", false, false},
		{"below_patch", "23.27.0.9.9", false, false},
		{"exact", "23.27.1.0.0", true, false},
		{"higher_patch", "23.27.1.0.1", true, false},
		{"higher_minor", "23.28.0.0.0", true, false},
		{"higher_major", "26.0.0.0.0", true, false},
		{"short_major", "26", true, false},
		{"short_minor", "23.27", false, false},
		{"short_patch", "23.27.1", true, false},
		{"four_components", "23.27.1.0", true, false},
		{"whitespace", " \t23.27.1.0.0\n", true, false},
		{"empty", "", false, true},
		{"empty_component", "23..1", false, true},
		{"trailing_dot", "23.27.1.", false, true},
		{"too_many", "23.27.1.0.0.0", false, true},
		{"text", "23.27.1beta", false, true},
		{"negative", "23.-27.1", false, true},
		{"signed", "+26", false, true},
		{"inner_space", "23. 27.1", false, true},
		{"overflow", "4294967296.0", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			supported, err := urowidCompatibleVersion(tc.value)
			if (err != nil) != tc.invalid || supported != tc.supported {
				t.Errorf("supported=%t, invalid=%t; want supported=%t, invalid=%t",
					supported, err != nil, tc.supported, tc.invalid)
			}
		})
	}
}

// urowidCompatibleVersion compares one to five numeric components with the minimum, padding
// missing components with zeroes.
func urowidCompatibleVersion(value string) (bool, error) {
	components := strings.Split(strings.TrimSpace(value), ".")
	if len(components) > 5 {
		return false, errors.New("compatible has more than five components")
	}
	// Missing components remain zero; compare numbers instead of version strings.
	var version [5]uint64
	for i, component := range components {
		if component == "" || strings.IndexFunc(component, func(c rune) bool { return c < '0' || c > '9' }) >= 0 {
			return false, errors.New("compatible components must contain decimal digits")
		}
		number, err := strconv.ParseUint(component, 10, 32)
		if err != nil {
			return false, errors.New("compatible component is too large")
		}
		version[i] = number
	}
	minimum := [5]uint64{23, 27, 1, 0, 0}
	for i := range minimum {
		if version[i] != minimum[i] {
			return version[i] > minimum[i], nil
		}
	}
	return true, nil
}

// requireCompatible skips unsupported databases and fails when the live parameter cannot be
// checked.
func (f *urowidFixture) requireCompatible(t *testing.T) {
	t.Helper()
	var value string
	err := f.conn.QueryRowContext(f.ctx,
		"SELECT value FROM v$parameter WHERE name = 'compatible'").Scan(&value)
	urowidRequireNoError(t, "PREREQUISITE: read live COMPATIBLE parameter", err)
	supported, err := urowidCompatibleVersion(value)
	if err != nil {
		t.Fatal("PREREQUISITE: live COMPATIBLE must contain one to five valid numeric components")
	}
	if !supported {
		t.Skipf("COMPATIBLE %s is below required %s; no UROWID resources created",
			strings.TrimSpace(value), urowidMinimumCompatible)
	}
}

// Fixture setup and session ownership.

// newUROWIDSession checks common prerequisites and reserves a session with automatic cleanup.
// It creates no tables or files, so the basic fetch test can prepare just its one-row input.
func newUROWIDSession(t *testing.T) *urowidFixture {
	t.Helper()
	if !urowidExplicitlySelected() {
		t.Skip("UROWID fixtures are opt-in; select a UROWID test/category or supply -test.urowid.directory")
	}
	if *urowidDirectory == "" {
		t.Fatal("PREREQUISITE: supply -test.urowid.directory with an existing writable DIRECTORY")
	}
	if TestingConfig == nil || !TestingConfig.Enabled {
		t.Fatal("PREREQUISITE: select an enabled configuration with -driver.config.filename and -driver.config.name")
	}
	dir := strings.ToUpper(strings.TrimSpace(*urowidDirectory))
	if !urowidIdentifier.MatchString(dir) {
		t.Fatal("PREREQUISITE: DIRECTORY must be a simple unquoted Oracle identifier")
	}
	f := &urowidFixture{dir: dir, prefix: "GUR_" + urowidRandom(t, 6)}
	f.ctx, f.cancel = context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(func() { f.cleanup(t) })
	f.db = openUROWIDDB(t)
	f.conn = urowidSession(t, f.db, f.ctx)

	// This live check must precede every fixture mutation, including file writes.
	f.requireCompatible(t)

	var sameSchema, regularUser, visibleDirectory int64
	err := f.conn.QueryRowContext(f.ctx, `
		SELECT CASE WHEN SYS_CONTEXT('USERENV','CURRENT_SCHEMA') =
		                      SYS_CONTEXT('USERENV','SESSION_USER') THEN 1 ELSE 0 END,
		       CASE WHEN SYS_CONTEXT('USERENV','SESSION_USER') = 'SYS' THEN 0 ELSE 1 END,
		       (SELECT COUNT(*) FROM all_directories WHERE directory_name = :dir)
		FROM dual`, sql.Named("dir", dir)).Scan(&sameSchema, &regularUser, &visibleDirectory)
	urowidRequireNoError(t, "check fixture prerequisites", err)
	if sameSchema != 1 || regularUser != 1 || visibleDirectory != 1 {
		t.Fatal("PREREQUISITE: use a regular test schema with the selected DIRECTORY visible")
	}
	return f
}

// newUROWIDFixture adds the four-row legacy/extended inputs used by the detailed driver tests.
func newUROWIDFixture(t *testing.T) *urowidFixture {
	t.Helper()
	f := newUROWIDSession(t)
	baseFile := f.prefix + "_base.csv"
	f.writeFile(t, baseFile, urowidBaseCSV)
	f.legacy = f.createExternal(t, "LEG", baseFile, false)
	f.extended = f.createExternal(t, "EXT", baseFile, true)
	// Inspect fixture formats only to ensure the driver receives the intended inputs.
	f.fixtureLocatorLength(t, f.legacy, 2, 4)
	f.fixtureLocatorLength(t, f.extended, 2, 6)
	return f
}

// urowidExplicitlySelected keeps server fixtures opt-in through a test pattern, category or
// explicit DIRECTORY flag.
func urowidExplicitlySelected() bool {
	for _, category := range strings.Split(flag.Lookup("test.category").Value.String(), ",") {
		if strings.EqualFold(strings.TrimSpace(category), "urowid") {
			return true
		}
	}
	if strings.Contains(strings.ToUpper(flag.Lookup("test.run").Value.String()), "UROWID") {
		return true
	}
	var directorySupplied bool
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "test.urowid.directory" {
			directorySupplied = true
		}
	})
	return directorySupplied
}

// urowidRandom returns random uppercase hexadecimal text for fixture names that do not collide
// across runs.
func urowidRandom(t *testing.T, size int) string {
	t.Helper()
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		t.Fatal("generate fixture name: random source unavailable")
	}
	return strings.ToUpper(hex.EncodeToString(b))
}

// openUROWIDDB opens the existing test configuration and limits the fixture pool to one connection.
func openUROWIDDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openTestDBWithConfig(TestingConfig)
	urowidRequireNoError(t, "open fixture database", err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

// urowidSession reserves the connection used by all operations within a test session.
func urowidSession(t *testing.T, db *sql.DB, ctx context.Context) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(ctx)
	urowidRequireNoError(t, "reserve physical session", err)
	return conn
}

// openFreshSession closes the previous pool and reserves a session from a new pool for
// independent subtests or retained-value reuse.
func (f *urowidFixture) openFreshSession(t *testing.T) {
	t.Helper()
	// A failed protocol read can make Close return another protocol error.
	// Retire that pool anyway; these are fixture operations, not connection
	// feature tests. The original native-read failure remains recorded.
	if err := f.conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		t.Logf("retire previous session: %s", urowidErrorSummary(err))
	}
	if err := f.db.Close(); err != nil {
		t.Logf("retire previous pool: %s", urowidErrorSummary(err))
	}
	f.db = openUROWIDDB(t)
	f.conn = urowidSession(t, f.db, f.ctx)
}

// Server resources: only generated names enter the cleanup ownership lists.

// execSQL executes fixture SQL on the reserved connection and reports a sanitized failure.
func (f *urowidFixture) execSQL(t *testing.T, operation, query string, args ...any) {
	t.Helper()
	_, err := f.conn.ExecContext(f.ctx, query, args...)
	urowidRequireNoError(t, operation, err)
}

// writeFile creates a new server CSV after checking for collisions, and closes the UTL_FILE
// handle even on errors.
func (f *urowidFixture) writeFile(t *testing.T, name, data string) {
	t.Helper()
	// Check before registering ownership: an existing file must never be removed.
	f.execSQL(t, "check source-file collision", `
		DECLARE exists_already BOOLEAN; file_size NUMBER; block_size BINARY_INTEGER;
		BEGIN
		  UTL_FILE.FGETATTR(:dir, :name, exists_already, file_size, block_size);
		  IF exists_already THEN
		    RAISE_APPLICATION_ERROR(-20001, 'UROWID fixture filename collision');
		  END IF;
		END;`, sql.Named("dir", f.dir), sql.Named("name", name))
	// Register before writing so cleanup removes a partially written owned file.
	f.files = append(f.files, name)
	f.execSQL(t, "write owned source file", `
		DECLARE
		  source_file UTL_FILE.FILE_TYPE;
		  contents VARCHAR2(32767) := :contents;
		  first_char PLS_INTEGER := 1;
		  newline_at PLS_INTEGER;
		BEGIN
		  source_file := UTL_FILE.FOPEN(:dir, :name, 'w', 32767);
		  WHILE first_char <= NVL(LENGTH(contents), 0) LOOP
		    newline_at := INSTR(contents, CHR(10), first_char);
		    IF newline_at = 0 THEN newline_at := LENGTH(contents) + 1; END IF;
		    UTL_FILE.PUT_LINE(source_file, SUBSTR(contents, first_char, newline_at-first_char));
		    first_char := newline_at + 1;
		  END LOOP;
		  UTL_FILE.FCLOSE(source_file);
		EXCEPTION WHEN OTHERS THEN
		  IF UTL_FILE.IS_OPEN(source_file) THEN UTL_FILE.FCLOSE(source_file); END IF;
		  RAISE;
		END;`, sql.Named("contents", data), sql.Named("dir", f.dir), sql.Named("name", name))
}

// createTable checks for a name collision and records ownership before DDL so partial setup can
// be cleaned.
func (f *urowidFixture) createTable(t *testing.T, name, ddl string) string {
	t.Helper()
	var existing int64
	err := f.conn.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM user_objects WHERE object_name = :name",
		sql.Named("name", name)).Scan(&existing)
	urowidRequireNoError(t, "check table-name collision", err)
	if existing != 0 {
		t.Fatal("generated table name already exists; it is not owned by this fixture")
	}
	// Reserve the checked, unique name before DDL. If the response is lost,
	// cleanup checks whether the table was actually created before dropping it.
	f.tables = append(f.tables, name)
	f.execSQL(t, "create owned "+name+" table", ddl)
	return name
}

// createExternal creates a single-file ORACLE_LOADER table using the requested locator format.
func (f *urowidFixture) createExternal(t *testing.T, suffix, file string, extended bool) string {
	t.Helper()
	name := f.prefix + "_" + suffix
	definition := "TYPE ORACLE_LOADER DEFAULT DIRECTORY \"" + f.dir + "\"\n" + urowidAccessParameters +
		"\nLOCATION (" + utils.EnquoteLiteral(file) + ")"
	if extended {
		definition += "\nWITH EXTENDED ROWID"
	}
	return f.createTable(t, name, "CREATE TABLE "+name+` (id NUMBER(10), payload VARCHAR2(40))
        ORGANIZATION EXTERNAL (`+definition+") REJECT LIMIT 0")
}

// createSavedTable persists two genuine locators in an ordinary UROWID column for native readback.
func (f *urowidFixture) createSavedTable(t *testing.T) string {
	t.Helper()
	name := f.prefix + "_SAVED"
	f.createTable(t, name, "CREATE TABLE "+name+" (id NUMBER(10) PRIMARY KEY, rid UROWID)")
	f.execSQL(t, "persist extended UROWIDs", "INSERT INTO "+name+" SELECT id, ROWID FROM "+f.extended+" WHERE id IN (2,4)")
	f.execSQL(t, "commit saved UROWIDs", "COMMIT")
	return name
}

// Native results and independent database-converted text.

// fetchNativeRow fetches a locator directly into a Go string and checks the accompanying row
// identity.
func (f *urowidFixture) fetchNativeRow(t *testing.T, table string, id int64) urowidRow {
	t.Helper()
	row := urowidRow{}
	err := f.conn.QueryRowContext(f.ctx,
		"SELECT ROWID, id, RTRIM(payload) FROM "+table+" WHERE id = :id",
		sql.Named("id", id)).Scan(&row.rid, &row.id, &row.payload)
	urowidRequireNoError(t, "native UROWID fetch", err)
	urowidCheckIdentity(t, row.id, row.payload, id)
	return row
}

// referenceText reads complete database-converted locator text independently of native ROWID
// decoding.
func (f *urowidFixture) referenceText(t *testing.T, table string, id int64) string {
	t.Helper()
	// A maximum locator can exceed SQL VARCHAR2(4000) after text encoding.
	// Convert inside PL/SQL, then fetch bounded VARCHAR2 chunks with explicit length.
	block := `DECLARE locator UROWID; locator_text VARCHAR2(32767);
        BEGIN
          SELECT ROWID INTO locator FROM ` + table + ` WHERE id = :id;
          locator_text := locator;
          :part := SUBSTR(locator_text, :offset, 2000);
          :total := LENGTH(locator_text);
        END;`
	var result strings.Builder
	var referenceLength int64
	for offset := 1; offset <= 32767; offset += 2000 {
		var part string
		var total int64
		_, err := f.conn.ExecContext(f.ctx, block,
			sql.Named("id", id),
			sql.Named("part", sql.Out{Dest: &part}),
			sql.Named("offset", offset),
			sql.Named("total", sql.Out{Dest: &total}))
		urowidRequireNoError(t, "database UROWID text reference", err)
		if offset == 1 {
			referenceLength = total
		}
		if total <= 0 || total > 32767 || total != referenceLength || len(part) == 0 {
			t.Fatal("database reference returned an invalid text length/chunk")
		}
		// Locator text is ASCII. Preserve every byte; do not trim or re-encode it.
		result.WriteString(part)
		if int64(result.Len()) == total {
			return result.String()
		}
		if int64(result.Len()) > total {
			t.Fatal("database reference chunk lengths exceed the reported length")
		}
	}
	t.Fatal("database reference did not return its complete text")
	return ""
}

// checkProjection checks native metadata and all four rows against same-query and complete text
// references.
func (f *urowidFixture) checkProjection(t *testing.T, table string, extended bool) {
	t.Helper()
	rows, err := f.conn.QueryContext(f.ctx,
		"SELECT ROWID AS rid, id, RTRIM(payload), CAST(ROWID AS VARCHAR2(4000)) FROM "+table+" ORDER BY id")
	urowidRequireNoError(t, "query UROWID projection", err)
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	urowidRequireNoError(t, "UROWID column metadata", err)
	// The current Go API reports both native variants as ROWID, not Node's type constants.
	if len(columns) != 4 || columns[0].Name() != "RID" || columns[0].DatabaseTypeName() != "ROWID" {
		t.Error("native RID metadata must identify RID with database type ROWID")
	}
	var result []urowidRow
	for rows.Next() {
		var row urowidRow
		urowidRequireNoError(t, "scan UROWID projection", rows.Scan(&row.rid, &row.id, &row.payload, &row.reference))
		result = append(result, row)
	}
	urowidRequireNoError(t, "fetch UROWID projection", rows.Err())
	urowidRequireNoError(t, "close UROWID projection", rows.Close())
	if len(result) != 4 {
		t.Fatalf("projection returned %d rows, want 4", len(result))
	}
	// Finish the reader before running reference queries on the same connection.
	for index, row := range result {
		urowidCheckIdentity(t, row.id, row.payload, int64(index+1))
		urowidCheckText(t, row.rid, row.reference)
		// Legacy locators require a reference from the same execution.
		if extended {
			urowidCheckText(t, row.rid, f.referenceText(t, table, row.id))
		}
	}
}

// Application lookups and result assertions.

// checkLookup binds a complete locator and requires exactly the intended fixture row.
func (f *urowidFixture) checkLookup(t *testing.T, table, rid string, expected int64) {
	t.Helper()
	f.checkQueryRows(t, "SELECT id, RTRIM(payload) FROM "+table+" WHERE ROWID = :rid",
		[]int64{expected}, sql.Named("rid", rid))
}

// checkQueryRows executes a locator-based query and checks its entire ordered result set.
func (f *urowidFixture) checkQueryRows(t *testing.T, query string, expected []int64, args ...any) {
	t.Helper()
	rows, err := f.conn.QueryContext(f.ctx, query, args...)
	urowidRequireNoError(t, "query UROWID-selected identities", err)
	urowidCheckRows(t, rows, expected)
}

// urowidCheckRows consumes and closes an ordinary or prepared result, checking missing and extra
// rows as well as values.
func urowidCheckRows(t *testing.T, rows *sql.Rows, expected []int64) {
	t.Helper()
	defer rows.Close()
	index := 0
	for rows.Next() {
		var id int64
		var payload string
		urowidRequireNoError(t, "scan UROWID-selected identity", rows.Scan(&id, &payload))
		if index >= len(expected) {
			t.Fatal("UROWID lookup returned an extra row")
		}
		urowidCheckIdentity(t, id, payload, expected[index])
		index++
	}
	urowidRequireNoError(t, "fetch UROWID-selected identities", rows.Err())
	urowidRequireNoError(t, "close UROWID-selected identities", rows.Close())
	if index != len(expected) {
		t.Errorf("UROWID lookup returned %d rows, want %d", index, len(expected))
	}
}

// urowidCheckIdentity checks the deterministic ID and payload expected for one fixture row.
func urowidCheckIdentity(t *testing.T, id int64, payload string, expected int64) {
	t.Helper()
	want := map[int64]string{1: "alpha", 2: "beta", 3: "gamma", 4: "delta"}[expected]
	if id != expected || payload != want {
		t.Errorf("row identity: got (%d,%q), want (%d,%q)", id, payload, expected, want)
	}
}

// urowidCheckText compares complete strings while keeping locator contents out of logs.
func urowidCheckText(t *testing.T, actual, expected string) {
	t.Helper()
	if actual != expected {
		t.Errorf("UROWID text differs from database reference (got %d bytes, want %d); locator values suppressed",
			len(actual), len(expected))
	}
}

// fixtureLocatorLength checks that a fixture supplies the intended subtype/length; it is an
// input prerequisite, not a server feature test.
func (f *urowidFixture) fixtureLocatorLength(t *testing.T, table string, id int64, expectedKind int) int {
	t.Helper()
	var dump string
	err := f.conn.QueryRowContext(f.ctx, "SELECT DUMP(ROWID,10,1,1) FROM "+table+" WHERE id = :id",
		sql.Named("id", id)).Scan(&dump)
	urowidRequireNoError(t, "PREREQUISITE: inspect locator fixture", err)
	match := urowidDumpHeader.FindStringSubmatch(dump)
	if len(match) != 3 {
		t.Fatal("PREREQUISITE: fixture DUMP must identify a type-208 UROWID")
	}
	length, lengthErr := strconv.Atoi(match[1])
	kind, kindErr := strconv.Atoi(match[2])
	if lengthErr != nil || kindErr != nil || kind != expectedKind {
		t.Fatal("PREREQUISITE: fixture locator subtype is unavailable or incorrect")
	}
	if (kind == 4 && length != 17) || (kind == 6 && (length <= 17 || length > urowidMaxRawLength)) {
		t.Fatal("PREREQUISITE: fixture locator has an unexpected internal length")
	}
	return length
}

// Error propagation: expected SQL errors remain errors, and details stay private.

// lookupError captures an error returned either when executing a negative query or while
// fetching its results.
func (f *urowidFixture) lookupError(t *testing.T, query string, args ...any) error {
	t.Helper()
	rows, err := f.conn.QueryContext(f.ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return urowidUnexpectedRow
	}
	if err := rows.Err(); err != nil {
		return err
	}
	urowidRequireNoError(t, "close negative lookup", rows.Close())
	return sql.ErrNoRows
}

// urowidExpectError requires the driver SQL-error type and the expected informative Oracle code.
func urowidExpectError(t *testing.T, err error, code string) {
	t.Helper()
	var sqlError oracleErrors.SQLError
	if !errors.As(err, &sqlError) || !strings.Contains(urowidErrorSummary(err), code) {
		t.Errorf("expected %s from the driver, got %s", code, urowidErrorSummary(err))
	}
}

// urowidRequireNoError stops a test with an operation name and a sanitized error summary.
func urowidRequireNoError(t *testing.T, operation string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %s", operation, urowidErrorSummary(err))
	}
}

// urowidErrorSummary reports error codes without printing credentials, connection descriptors or
// locator values.
func urowidErrorSummary(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, sql.ErrNoRows) {
		return "NO_ROWS"
	}
	if errors.Is(err, urowidUnexpectedRow) {
		return "ROW_RETURNED"
	}
	codes := urowidErrorCode.FindAllString(err.Error(), -1)
	if len(codes) != 0 {
		return strings.Join(codes, "/")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context deadline exceeded"
	}
	return fmt.Sprintf("non-Oracle error (%T)", err)
}

// Cleanup: drop owned tables before removing their owned source files.

// cleanup closes the test session and uses a fresh connection to remove only resources recorded
// as owned.
func (f *urowidFixture) cleanup(t *testing.T) {
	t.Helper()
	if f.cancel != nil {
		f.cancel()
	}
	if f.conn != nil {
		_ = f.conn.Close()
	}
	if f.db != nil {
		_ = f.db.Close()
	}
	// A compatibility skip reaches here with no mutations to undo.
	if len(f.tables) == 0 && len(f.files) == 0 {
		return
	}
	// A bad locator can poison a session. Teardown always uses a fresh one.
	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Errorf("CLEANUP failed to connect: %s; owned prefix %s", urowidErrorSummary(err), f.prefix)
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Keep files if any owned table still needs them after an incomplete teardown.
	allDropped := true
	for i := len(f.tables) - 1; i >= 0; i-- {
		var existing int64
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user_tables WHERE table_name = :name",
			sql.Named("name", f.tables[i])).Scan(&existing)
		if err != nil {
			allDropped = false
			t.Errorf("CLEANUP inspect table %s: %s", f.tables[i], urowidErrorSummary(err))
			continue
		}
		if existing == 0 {
			continue
		}
		if err := dropTable(ctx, db, f.tables[i]); err != nil {
			allDropped = false
			t.Errorf("CLEANUP table %s: %s", f.tables[i], urowidErrorSummary(err))
		}
	}
	if !allDropped {
		t.Error("CLEANUP retained owned source files because table removal was incomplete")
		return
	}
	cleanupOK := true
	for _, name := range f.files {
		_, err := db.ExecContext(ctx, `
			DECLARE file_exists BOOLEAN; file_size NUMBER; block_size BINARY_INTEGER;
			BEGIN
			  UTL_FILE.FGETATTR(:dir, :name, file_exists, file_size, block_size);
			  IF file_exists THEN UTL_FILE.FREMOVE(:dir, :name); END IF;
			END;`, sql.Named("dir", f.dir), sql.Named("name", name))
		if err != nil {
			cleanupOK = false
			t.Errorf("CLEANUP file %s: %s", name, urowidErrorSummary(err))
		}
	}
	if cleanupOK {
		t.Log("CLEANUP=OK")
	}
}
