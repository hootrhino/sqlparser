package sqlparser

import (
	"fmt"
	"log"
	"os"
	"testing"
	"text/template"

	"github.com/hootrhino/sqlparser/query"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name     string
	SQL      string
	Expected query.Query
	Err      error
}

type output struct {
	NoErrorExamples []testCase
	ErrorExamples   []testCase
	Types           []string
	Operators       []string
}

func TestSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "empty query fails",
			SQL:      "",
			Expected: query.Query{},
			Err:      fmt.Errorf("query type cannot be empty"),
		},
		{
			Name:     "SELECT without FROM fails",
			SQL:      "SELECT",
			Expected: query.Query{Type: query.Select},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "SELECT without fields fails",
			SQL:      "SELECT FROM 'a'",
			Expected: query.Query{Type: query.Select},
			Err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			Name:     "SELECT with comma and empty field fails",
			SQL:      "SELECT b, FROM 'a'",
			Expected: query.Query{Type: query.Select},
			Err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			Name:     "SELECT works",
			SQL:      "SELECT a FROM 'b'",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a"}},
			Err:      nil,
		},
		{
			Name:     "SELECT works with lowercase",
			SQL:      "select a fRoM 'b'",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a"}},
			Err:      nil,
		},
		{
			Name:     "SELECT many fields works",
			SQL:      "SELECT a, c, d FROM 'b'",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      nil,
		},
		{
			Name: "SELECT with alias works",
			SQL:  "SELECT a as z, b as y, c FROM 'b'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "b", "c"},
				Aliases: map[string]string{
					"a": "z",
					"b": "y",
				},
			},
			Err: nil,
		},

		{
			Name:     "SELECT with empty WHERE fails",
			SQL:      "SELECT a, c, d FROM 'b' WHERE",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			Name:     "SELECT with WHERE with only operand fails",
			SQL:      "SELECT a, c, d FROM 'b' WHERE a",
			Expected: query.Query{Type: query.Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			Name: "SELECT with WHERE with = works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a = ''",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with < works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a < '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Lt, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with <= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a <= '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Lte, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with > works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a > '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Gt, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with >= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a >= '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Gte, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with != works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Ne, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with != works (comparing field against another field)",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != b",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Ne, Operand2: "b", Operand2IsField: true},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT * works",
			SQL:  "SELECT * FROM 'b'",
			Expected: query.Query{
				Type:       query.Select,
				TableName:  "b",
				Fields:     []string{"*"},
				Conditions: nil,
			},
			Err: nil,
		},
		{
			Name: "SELECT a, * works",
			SQL:  "SELECT a, * FROM 'b'",
			Expected: query.Query{
				Type:       query.Select,
				TableName:  "b",
				Fields:     []string{"a", "*"},
				Conditions: nil,
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with two conditions using AND works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1' AND b = '2'",
			Expected: query.Query{
				Type:      query.Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Ne, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "2", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name:     "Empty UPDATE fails",
			SQL:      "UPDATE",
			Expected: query.Query{},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "Incomplete UPDATE with table name fails",
			SQL:      "UPDATE 'a'",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			Name:     "Incomplete UPDATE with table name and SET fails",
			SQL:      "UPDATE 'a' SET",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			Name:     "Incomplete UPDATE with table name, SET with a field but no value and WHERE fails",
			SQL:      "UPDATE 'a' SET b WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at UPDATE: expected '='"),
		},
		{
			Name:     "Incomplete UPDATE with table name, SET with a field and = but no value and WHERE fails",
			SQL:      "UPDATE 'a' SET b = WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at UPDATE: expected quoted value"),
		},
		{
			Name:     "Incomplete UPDATE due to no WHERE clause fails",
			SQL:      "UPDATE 'a' SET b = 'hello' WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			Name:     "Incomplete UPDATE due incomplete WHERE clause fails",
			SQL:      "UPDATE 'a' SET b = 'hello' WHERE a",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			Name: "UPDATE works",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE a = '1'",
			Expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE works with simple quote inside",
			SQL:  "UPDATE 'a' SET b = 'hello\\'world' WHERE a = '1'",
			Expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello\\'world"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE with multiple SETs works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1'",
			Expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE with multiple SETs and multiple conditions works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1' AND b = '789'",
			Expected: query.Query{
				Type:      query.Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []query.Condition{
					{Operand1: "a", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "789", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name:     "Empty DELETE fails",
			SQL:      "DELETE FROM",
			Expected: query.Query{},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "DELETE without WHERE fails",
			SQL:      "DELETE FROM 'a'",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			Name:     "DELETE with empty WHERE fails",
			SQL:      "DELETE FROM 'a' WHERE",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: empty WHERE clause"),
		},
		{
			Name:     "DELETE with WHERE with field but no operator fails",
			SQL:      "DELETE FROM 'a' WHERE b",
			Expected: query.Query{},
			Err:      fmt.Errorf("at WHERE: condition without operator"),
		},
		{
			Name: "DELETE with WHERE works",
			SQL:  "DELETE FROM 'a' WHERE b = '1'",
			Expected: query.Query{
				Type:      query.Delete,
				TableName: "a",
				Conditions: []query.Condition{
					{Operand1: "b", Operand1IsField: true, Operator: query.Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name:     "Empty INSERT fails",
			SQL:      "INSERT INTO",
			Expected: query.Query{},
			Err:      fmt.Errorf("table name cannot be empty"),
		},
		{
			Name:     "INSERT with no rows to insert fails",
			SQL:      "INSERT INTO 'a'",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails",
			SQL:      "INSERT INTO 'a' (",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails #2",
			SQL:      "INSERT INTO 'a' (b",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails #3",
			SQL:      "INSERT INTO 'a' (b)",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails #4",
			SQL:      "INSERT INTO 'a' (b) VALUES",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: need at least one row to insert"),
		},
		{
			Name:     "INSERT with incomplete row fails",
			SQL:      "INSERT INTO 'a' (b) VALUES (",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: value count doesn't match field count"),
		},
		{
			Name: "INSERT works",
			SQL:  "INSERT INTO 'a' (b) VALUES ('1')",
			Expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]string{{"1"}},
			},
			Err: nil,
		},
		{
			Name:     "INSERT * fails",
			SQL:      "INSERT INTO 'a' (*) VALUES ('1')",
			Expected: query.Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected at least one field to insert"),
		},
		{
			Name: "INSERT with multiple fields works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' )",
			Expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}},
			},
			Err: nil,
		},
		{
			Name: "INSERT with multiple fields and multiple values works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' ),('4','5' ,'6' )",
			Expected: query.Query{
				Type:      query.Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
			},
			Err: nil,
		},
		{
			Name: "CREATE TABLE",
			SQL:  "CREATE TABLE test (name string, age number, gender bool)",
			Expected: query.Query{
				Type:      query.Create,
				TableName: "test",
				CreateFields: map[string]string{
					"name":   "string",
					"age":    "number",
					"gender": "bool",
				},
			},
			Err: nil,
		},
	}

	output := output{Types: query.TypeString, Operators: query.OperatorString}
	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			actual, err := ParseMany([]string{tc.SQL})
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err.Error(), err.Error(), "Unexpected error")
			}
			if len(actual) > 0 {
				require.Equal(t, tc.Expected, actual[0], "Query didn't match expectation")
			}
			if tc.Err != nil {
				output.ErrorExamples = append(output.ErrorExamples, tc)
			} else {
				output.NoErrorExamples = append(output.NoErrorExamples, tc)
			}
		})
	}
	createReadme(output)
}

func createReadme(out output) {
	content, err := os.ReadFile("README.template")
	if err != nil {
		log.Fatal(err)
	}
	t := template.Must(template.New("").Parse(string(content)))
	f, err := os.Create("README.md")
	if err != nil {
		log.Fatal(err)
	}
	if err := t.Execute(f, out); err != nil {
		log.Fatal(err)
	}
}

// Test LIKE and IN operators
func TestParseLikeAndInOperators(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected query.Query
		hasError bool
	}{
		{
			name: "SELECT with LIKE operator",
			sql:  "SELECT name FROM users WHERE name LIKE 'John%'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "users",
				Fields:    []string{"name"},
				Conditions: []query.Condition{
					{
						Operand1:        "name",
						Operand1IsField: true,
						Operator:        query.Like,
						Operand2:        "John%",
						Operand2IsField: false,
					},
				},
			},
			hasError: false,
		},
		{
			name: "SELECT with NOT LIKE operator",
			sql:  "SELECT * FROM products WHERE name NOT LIKE '%test%'",
			expected: query.Query{
				Type:      query.Select,
				TableName: "products",
				Fields:    []string{"*"},
				Conditions: []query.Condition{
					{
						Operand1:        "name",
						Operand1IsField: true,
						Operator:        query.NotLike,
						Operand2:        "%test%",
						Operand2IsField: false,
					},
				},
			},
			hasError: false,
		},
		{
			name: "DELETE with LIKE operator",
			sql:  "DELETE FROM logs WHERE message LIKE 'Error:%'",
			expected: query.Query{
				Type:      query.Delete,
				TableName: "logs",
				Conditions: []query.Condition{
					{
						Operand1:        "message",
						Operand1IsField: true,
						Operator:        query.Like,
						Operand2:        "Error:%",
						Operand2IsField: false,
					},
				},
			},
			hasError: false,
		},
		{
			name: "UPDATE with LIKE operator",
			sql:  "UPDATE products SET price = '99' WHERE name LIKE 'Pro%'",
			expected: query.Query{
				Type:      query.Update,
				TableName: "products",
				Updates:   map[string]string{"price": "99"},
				Conditions: []query.Condition{
					{
						Operand1:        "name",
						Operand1IsField: true,
						Operator:        query.Like,
						Operand2:        "Pro%",
						Operand2IsField: false,
					},
				},
			},
			hasError: false,
		},
		{
			name: "SELECT with IN operator",
			sql:  "SELECT name FROM users WHERE id IN ('1', '2', '3')",
			expected: query.Query{
				Type:      query.Select,
				TableName: "users",
				Fields:    []string{"name"},
				Conditions: []query.Condition{
					{
						Operand1:        "id",
						Operand1IsField: true,
						Operator:        query.In,
						InValues:        []string{"1", "2", "3"},
					},
				},
			},
			hasError: false,
		},
		{
			name: "SELECT with NOT IN operator",
			sql:  "SELECT * FROM products WHERE status NOT IN ('sold', 'discontinued')",
			expected: query.Query{
				Type:      query.Select,
				TableName: "products",
				Fields:    []string{"*"},
				Conditions: []query.Condition{
					{
						Operand1:        "status",
						Operand1IsField: true,
						Operator:        query.NotIn,
						InValues:        []string{"sold", "discontinued"},
					},
				},
			},
			hasError: false,
		},
		{
			name: "DELETE with IN operator",
			sql:  "DELETE FROM logs WHERE level IN ('INFO', 'DEBUG')",
			expected: query.Query{
				Type:      query.Delete,
				TableName: "logs",
				Conditions: []query.Condition{
					{
						Operand1:        "level",
						Operand1IsField: true,
						Operator:        query.In,
						InValues:        []string{"INFO", "DEBUG"},
					},
				},
			},
			hasError: false,
		},
		{
			name: "UPDATE with IN operator",
			sql:  "UPDATE users SET active = 'false' WHERE id IN ('10', '20')",
			expected: query.Query{
				Type:      query.Update,
				TableName: "users",
				Updates:   map[string]string{"active": "false"},
				Conditions: []query.Condition{
					{
						Operand1:        "id",
						Operand1IsField: true,
						Operator:        query.In,
						InValues:        []string{"10", "20"},
					},
				},
			},
			hasError: false,
		},
		{
			name:     "IN operator with empty values fails",
			sql:      "SELECT * FROM users WHERE id IN ()",
			expected: query.Query{},
			hasError: true,
		},
		{
			name:     "IN operator with incomplete values fails",
			sql:      "SELECT * FROM users WHERE id IN ('1', '2'",
			expected: query.Query{},
			hasError: true,
		},
		{
			name:     "LIKE operator with incomplete value fails",
			sql:      "SELECT * FROM users WHERE name LIKE 'John",
			expected: query.Query{},
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)

			if tt.hasError {
				require.Error(t, err, "Expected an error but got none")
			} else {
				require.NoError(t, err, "Unexpected error")
				require.Equal(t, tt.expected, result, "Query didn't match expectation")
			}
		})
	}
}

// Test LIKE and IN operators
func Test_IN_operator_without_opening_parenthesis_fails(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected query.Query
		hasError bool
	}{
		{
			name:     "IN operator without opening parenthesis fails",
			sql:      "SELECT * FROM users WHERE id IN '1', '2'",
			expected: query.Query{},
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.hasError {
				if err == nil {
					t.Error(err)
				}
				t.Log(err)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	// Sample data for testing, using map[string]any for values
	data := map[string]map[string]any{
		"1": {"id": "1", "name": "John Doe", "age": "30", "status": "active", "city": "New York",
			"address": map[string]any{"street": "123 Main St", "zip": "10001"}},
		"2": {"id": "2", "name": "Jane Smith", "age": "25", "status": "inactive", "city": "Los Angeles",
			"address": map[string]any{"street": "456 Oak Ave", "zip": "90001"}},
		"3": {"id": "3", "name": "Peter Jones", "age": "35", "status": "active", "city": "New York",
			"address": map[string]any{"street": "789 Pine Ln", "zip": "10001"}},
		"4": {"id": "4", "name": "David Lee", "age": "40", "status": "active", "city": "Chicago",
			"address": map[string]any{"street": "321 Elm Dr", "zip": "60601"}},
		"5": {"id": "5", "name": "John Smith", "age": "28", "status": "inactive", "city": "New York",
			"address": map[string]any{"street": "987 Cedar Rd", "zip": "10001"}},
	}

	tests := []struct {
		name        string
		sql         string
		expected    map[string]map[string]any
		expectedErr string
	}{
		{
			name:        "SELECT with '=' operator",
			sql:         "SELECT * FROM users WHERE status = 'active'",
			expected:    map[string]map[string]any{"1": data["1"], "3": data["3"], "4": data["4"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with '!=' operator",
			sql:         "SELECT * FROM users WHERE status != 'active'",
			expected:    map[string]map[string]any{"2": data["2"], "5": data["5"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with '>' operator (string comparison)",
			sql:         "SELECT * FROM users WHERE age > '30'",
			expected:    map[string]map[string]any{"3": data["3"], "4": data["4"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with '<' operator (string comparison)",
			sql:         "SELECT * FROM users WHERE age < '30'",
			expected:    map[string]map[string]any{"2": data["2"], "5": data["5"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with 'AND' condition",
			sql:         "SELECT * FROM users WHERE status = 'active' AND city = 'New York'",
			expected:    map[string]map[string]any{"1": data["1"], "3": data["3"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with LIKE operator",
			sql:         "SELECT * FROM users WHERE name LIKE 'John%'",
			expected:    map[string]map[string]any{"1": data["1"], "5": data["5"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with NOT LIKE operator",
			sql:         "SELECT * FROM users WHERE name NOT LIKE 'John%'",
			expected:    map[string]map[string]any{"2": data["2"], "3": data["3"], "4": data["4"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with IN operator",
			sql:         "SELECT * FROM users WHERE id IN ('1', '3', '5')",
			expected:    map[string]map[string]any{"1": data["1"], "3": data["3"], "5": data["5"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with NOT IN operator",
			sql:         "SELECT * FROM users WHERE id NOT IN ('1', '3', '5')",
			expected:    map[string]map[string]any{"2": data["2"], "4": data["4"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with no matching results",
			sql:         "SELECT * FROM users WHERE age = '50'",
			expected:    map[string]map[string]any{},
			expectedErr: "",
		},
		{
			name:        "Invalid SQL syntax",
			sql:         "SELECT * FROM users WHERE age = ",
			expected:    nil,
			expectedErr: "failed to parse SQL: at WHERE: expected quoted value",
		},
		{
			name:        "SELECT with no WHERE clause",
			sql:         "SELECT * FROM users",
			expected:    data,
			expectedErr: "",
		},
		{
			name:        "SELECT with Gt on non-existent field",
			sql:         "SELECT * FROM users WHERE non_existent_field > '10'",
			expected:    map[string]map[string]any{},
			expectedErr: "",
		},
		{
			name:        "SELECT with IN on non-existent field",
			sql:         "SELECT * FROM users WHERE non_existent_field IN ('a')",
			expected:    map[string]map[string]any{},
			expectedErr: "",
		},
		{
			name:        "SELECT with IN on non-matching values",
			sql:         "SELECT * FROM users WHERE id IN ('6', '7')",
			expected:    map[string]map[string]any{},
			expectedErr: "",
		},
		{
			name:        "SELECT with nested field",
			sql:         "SELECT * FROM users WHERE address.zip = '10001'",
			expected:    map[string]map[string]any{"1": data["1"], "3": data["3"], "5": data["5"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with nested field and AND",
			sql:         "SELECT * FROM users WHERE address.zip = '10001' AND status = 'active'",
			expected:    map[string]map[string]any{"1": data["1"], "3": data["3"]},
			expectedErr: "",
		},
		{
			name:        "SELECT with non-existent nested field",
			sql:         "SELECT * FROM users WHERE address.non_existent_field = '123'",
			expected:    map[string]map[string]any{},
			expectedErr: "",
		},
		{
			name:        "SELECT with partially nested non-map field",
			sql:         "SELECT * FROM users WHERE id.non_existent_field = '123'",
			expected:    map[string]map[string]any{},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Filter(tt.sql, data)
			if err != nil {
				t.Error(err)
			}
		})
	}
}
