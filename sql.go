package sqlparser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Parse takes a string representing a SQL query and parses it into a Query struct. It may fail.
func Parse(sqls string) (Query, error) {
	qs, err := ParseMany([]string{sqls})
	if len(qs) == 0 {
		return Query{}, err
	}
	return qs[0], err
}

// ParseMany takes a string slice representing many SQL queries and parses them into a Query struct slice.
// It may fail. If it fails, it will stop at the first failure.
func ParseMany(sqls []string) ([]Query, error) {
	qs := []Query{}
	for _, sql := range sqls {
		q, err := parse(sql)
		if err != nil {
			return qs, err
		}
		qs = append(qs, q)
	}
	return qs, nil
}

func parse(sql string) (Query, error) {
	return (&parser{0, strings.TrimSpace(sql), stepType, Query{}, nil, ""}).parse()
}

type step int

const (
	stepType step = iota
	stepSelectField
	stepSelectFrom
	stepSelectComma
	stepSelectFromTable
	stepInsertTable
	stepInsertFieldsOpeningParens
	stepInsertFields
	stepInsertFieldsCommaOrClosingParens
	stepInsertValuesOpeningParens
	stepInsertValuesRWord
	stepInsertValues
	stepInsertValuesCommaOrClosingParens
	stepInsertValuesCommaBeforeOpeningParens
	stepUpdateTable
	stepUpdateSet
	stepUpdateField
	stepUpdateEquals
	stepUpdateValue
	stepUpdateComma
	stepDeleteFromTable
	stepWhere
	stepWhereField
	stepWhereOperator
	stepWhereValue
	stepWhereAnd
	stepCreateTable
	stepParseCreateFields //()
	stepWhereInOpeningParens
	stepWhereInValue
	stepWhereInCommaOrClosingParens
)

type parser struct {
	i               int
	sql             string
	step            step
	query           Query
	err             error
	nextUpdateField string
}

func (p *parser) parse() (Query, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) doParse() (Query, error) {
	for {
		if p.i >= len(p.sql) {
			return p.query, p.err
		}
		switch p.step {
		case stepType:
			QType := strings.ToUpper(p.peek())
			switch QType {
			case "SELECT":
				p.query.Type = Select
				p.pop()
				p.step = stepSelectField
			case "INSERT INTO":
				p.query.Type = Insert
				p.pop()
				p.step = stepInsertTable
			case "UPDATE":
				p.query.Type = Update
				p.query.Updates = map[string]string{}
				p.pop()
				p.step = stepUpdateTable
			case "DELETE FROM":
				p.query.Type = Delete
				p.pop()
				p.step = stepDeleteFromTable
			case "CREATE TABLE":
				p.query.Type = Create
				p.pop()
				p.step = stepCreateTable
				p.query.CreateFields = map[string]string{}
			default:
				return p.query, fmt.Errorf("invalid query type")
			}
		case stepCreateTable:
			tableName := p.peek()
			if tableName == "" {
				return p.query, fmt.Errorf("missing table name")
			}
			p.query.TableName = tableName
			p.pop()
			leftBracket := p.peek() // (
			if leftBracket != "(" {
				return p.query, fmt.Errorf("syntax error, expect '(")
			}
			p.step = stepParseCreateFields
			p.pop()
		case stepParseCreateFields:
			field := p.peek()
			if field == "" {
				return p.query, fmt.Errorf("syntax error, expect filed name")
			}
			p.pop()
			Type := p.peek()
			if Type == "" {
				return p.query, fmt.Errorf("syntax error, expect filed type")
			}
			p.pop()
			p.query.CreateFields[field] = Type
			NToken := p.peek()
			switch NToken {
			case ",":
				p.pop()
				p.step = stepParseCreateFields
			case ")":
				p.pop()
			default:
				return p.query, fmt.Errorf("syntax error, expect ')'")
			}
		case stepSelectField:
			identifier := p.peek()
			if !isIdentifierOrAsterisk(identifier) {
				return p.query, fmt.Errorf("at SELECT: expected field to SELECT")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			maybeFrom := p.peek()
			if strings.ToUpper(maybeFrom) == "AS" {
				p.pop()
				alias := p.peek()
				if !isIdentifier(alias) {
					return p.query, fmt.Errorf("at SELECT: expected field alias for \"" + identifier + " as\" to SELECT")
				}
				if p.query.Aliases == nil {
					p.query.Aliases = make(map[string]string)
				}
				p.query.Aliases[identifier] = alias
				p.pop()
				maybeFrom = p.peek()
			}
			if strings.ToUpper(maybeFrom) == "FROM" {
				p.step = stepSelectFrom
				continue
			}
			p.step = stepSelectComma
		case stepSelectComma:
			commaRWord := p.peek()
			if commaRWord != "," {
				return p.query, fmt.Errorf("at SELECT: expected comma or FROM")
			}
			p.pop()
			p.step = stepSelectField
		case stepSelectFrom:
			fromRWord := p.peek()
			if strings.ToUpper(fromRWord) != "FROM" {
				return p.query, fmt.Errorf("at SELECT: expected FROM")
			}
			p.pop()
			p.step = stepSelectFromTable
		case stepSelectFromTable:
			tableName := p.peek()
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at SELECT: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepWhere
		case stepInsertTable:
			tableName := p.peek()
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at INSERT INTO: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepInsertFieldsOpeningParens
		case stepDeleteFromTable:
			tableName := p.peek()
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at DELETE FROM: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepWhere
		case stepUpdateTable:
			tableName := p.peek()
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at UPDATE: expected quoted table name")
			}
			p.query.TableName = tableName
			p.pop()
			p.step = stepUpdateSet
		case stepUpdateSet:
			setRWord := p.peek()
			if setRWord != "SET" {
				return p.query, fmt.Errorf("at UPDATE: expected 'SET'")
			}
			p.pop()
			p.step = stepUpdateField
		case stepUpdateField:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at UPDATE: expected at least one field to update")
			}
			p.nextUpdateField = identifier
			p.pop()
			p.step = stepUpdateEquals
		case stepUpdateEquals:
			equalsRWord := p.peek()
			if equalsRWord != "=" {
				return p.query, fmt.Errorf("at UPDATE: expected '='")
			}
			p.pop()
			p.step = stepUpdateValue
		case stepUpdateValue:
			quotedValue, ln := p.peekQuotedStringWithLength()
			if ln == 0 {
				return p.query, fmt.Errorf("at UPDATE: expected quoted value")
			}
			p.query.Updates[p.nextUpdateField] = quotedValue
			p.nextUpdateField = ""
			p.pop()
			maybeWhere := p.peek()
			if strings.ToUpper(maybeWhere) == "WHERE" {
				p.step = stepWhere
				continue
			}
			p.step = stepUpdateComma
		case stepUpdateComma:
			commaRWord := p.peek()
			if commaRWord != "," {
				return p.query, fmt.Errorf("at UPDATE: expected ','")
			}
			p.pop()
			p.step = stepUpdateField
		case stepWhere:
			whereRWord := p.peek()
			if strings.ToUpper(whereRWord) != "WHERE" {
				return p.query, fmt.Errorf("expected WHERE")
			}
			p.pop()
			p.step = stepWhereField
		case stepWhereField:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at WHERE: expected field")
			}
			p.query.Conditions = append(p.query.Conditions, Condition{Operand1: identifier, Operand1IsField: true})
			p.pop()
			p.step = stepWhereOperator
		case stepWhereOperator:
			operator := p.peek()
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			switch operator {
			case "=":
				currentCondition.Operator = Eq
			case ">":
				currentCondition.Operator = Gt
			case ">=":
				currentCondition.Operator = Gte
			case "<":
				currentCondition.Operator = Lt
			case "<=":
				currentCondition.Operator = Lte
			case "!=":
				currentCondition.Operator = Ne
			case "LIKE":
				currentCondition.Operator = Like
			case "NOT LIKE":
				currentCondition.Operator = NotLike
			case "IN":
				currentCondition.Operator = In
			case "NOT IN":
				currentCondition.Operator = NotIn
			default:
				return p.query, fmt.Errorf("at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()

			// For IN and NOT IN operators, expect opening parenthesis
			if currentCondition.Operator == In || currentCondition.Operator == NotIn {
				p.step = stepWhereInOpeningParens
			} else {
				p.step = stepWhereValue
			}
		case stepWhereInOpeningParens:
			openingParens := p.peek()
			if openingParens != "(" {
				return p.query, fmt.Errorf("at WHERE IN: expected opening parenthesis")
			}
			p.pop()
			p.step = stepWhereInValue
		case stepWhereInValue:
			quotedValue, ln := p.peekQuotedStringWithLength()
			if ln == 0 {
				return p.query, fmt.Errorf("at WHERE IN: expected quoted value")
			}
			currentCondition := &p.query.Conditions[len(p.query.Conditions)-1]
			currentCondition.InValues = append(currentCondition.InValues, quotedValue)
			p.pop()
			p.step = stepWhereInCommaOrClosingParens
		case stepWhereInCommaOrClosingParens:
			commaOrClosingParens := p.peek()
			if commaOrClosingParens == "" {
				return p.query, fmt.Errorf("at WHERE IN: expected closing parenthesis")
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepWhereInValue
				continue
			} else if commaOrClosingParens == ")" {
				p.step = stepWhereAnd
				continue
			} else {
				return p.query, fmt.Errorf("at WHERE IN: expected comma or closing parenthesis")
			}
		case stepWhereValue:
			currentCondition := &p.query.Conditions[len(p.query.Conditions)-1]
			// For LIKE and NOT LIKE, the operand must be a quoted string.
			if currentCondition.Operator == Like || currentCondition.Operator == NotLike {
				quotedValue, ln := p.peekQuotedStringWithLength()
				if ln == 0 {
					return p.query, fmt.Errorf("at WHERE: expected quoted value for LIKE/NOT LIKE")
				}
				currentCondition.Operand2 = quotedValue
				currentCondition.Operand2IsField = false
			} else {
				// For other operators, it can be an identifier or a quoted string.
				identifier := p.peek()
				if isIdentifier(identifier) {
					currentCondition.Operand2 = identifier
					currentCondition.Operand2IsField = true
				} else {
					quotedValue, ln := p.peekQuotedStringWithLength()
					if ln == 0 {
						return p.query, fmt.Errorf("at WHERE: expected quoted value")
					}
					currentCondition.Operand2 = quotedValue
					currentCondition.Operand2IsField = false
				}
			}
			p.pop()
			p.step = stepWhereAnd
		case stepWhereAnd:
			andRWord := p.peek()
			if strings.ToUpper(andRWord) != "AND" {
				return p.query, fmt.Errorf("expected AND")
			}
			p.pop()
			p.step = stepWhereField
		case stepInsertFieldsOpeningParens:
			openingParens := p.peek()
			if len(openingParens) != 1 || openingParens != "(" {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.pop()
			p.step = stepInsertFields
		case stepInsertFields:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at INSERT INTO: expected at least one field to insert")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			p.step = stepInsertFieldsCommaOrClosingParens
		case stepInsertFieldsCommaOrClosingParens:
			commaOrClosingParens := p.peek()
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepInsertFields
				continue
			}
			p.step = stepInsertValuesRWord
		case stepInsertValuesRWord:
			valuesRWord := p.peek()
			if strings.ToUpper(valuesRWord) != "VALUES" {
				return p.query, fmt.Errorf("at INSERT INTO: expected 'VALUES'")
			}
			p.pop()
			p.step = stepInsertValuesOpeningParens
		case stepInsertValuesOpeningParens:
			openingParens := p.peek()
			if openingParens != "(" {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.query.Inserts = append(p.query.Inserts, []string{})
			p.pop()
			p.step = stepInsertValues
		case stepInsertValues:
			quotedValue, ln := p.peekQuotedStringWithLength()
			if ln == 0 {
				return p.query, fmt.Errorf("at INSERT INTO: expected quoted value")
			}
			p.query.Inserts[len(p.query.Inserts)-1] = append(p.query.Inserts[len(p.query.Inserts)-1], quotedValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
		case stepInsertValuesCommaOrClosingParens:
			commaOrClosingParens := p.peek()
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepInsertValues
				continue
			}
			currentInsertRow := p.query.Inserts[len(p.query.Inserts)-1]
			if len(currentInsertRow) < len(p.query.Fields) {
				return p.query, fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
			p.step = stepInsertValuesCommaBeforeOpeningParens
		case stepInsertValuesCommaBeforeOpeningParens:
			commaRWord := p.peek()
			if strings.ToUpper(commaRWord) != "," {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma")
			}
			p.pop()
			p.step = stepInsertValuesOpeningParens
		}
	}
}

func (p *parser) peek() string {
	peeked, _ := p.peekWithLength()
	return peeked
}

func (p *parser) pop() string {
	peeked, len := p.peekWithLength()
	p.i += len
	p.popWhitespace()
	return peeked
}

func (p *parser) popWhitespace() {
	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

var reservedWords = []string{
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	"WHERE", "FROM", "SET", "AS", "CREATE TABLE", "LIKE", "NOT LIKE", "IN", "NOT IN",
}

func (p *parser) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}
	if p.sql[p.i] == '\'' { // Quoted string
		return p.peekQuotedStringWithLength()
	}
	return p.peekIdentifierWithLength()
}

func (p *parser) peekQuotedStringWithLength() (string, int) {
	if len(p.sql) < p.i || p.sql[p.i] != '\'' {
		return "", 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if p.sql[i] == '\'' && p.sql[i-1] != '\\' {
			return p.sql[p.i+1 : i], len(p.sql[p.i+1:i]) + 2 // +2 for the two quotes
		}
	}
	return "", 0
}

func (p *parser) peekIdentifierWithLength() (string, int) {
	start := p.i
	for i := start; i < len(p.sql); i++ {
		ch := p.sql[i]
		if !(ch >= 'a' && ch <= 'z' ||
			ch >= 'A' && ch <= 'Z' ||
			ch >= '0' && ch <= '9' ||
			ch == '_' || ch == '*' || ch == '.') {
			return p.sql[start:i], i - start
		}
	}
	return p.sql[start:], len(p.sql) - start
}

func (p *parser) validate() error {
	if len(p.query.Conditions) == 0 && p.step == stepWhereField {
		return fmt.Errorf("at WHERE: empty WHERE clause")
	}
	if p.query.Type == UnknownType {
		return fmt.Errorf("query type cannot be empty")
	}
	if p.query.Type == Create {
		return nil
	}
	if p.query.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(p.query.Conditions) == 0 && (p.query.Type == Update || p.query.Type == Delete) {
		return fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == UnknownOperator {
			return fmt.Errorf("at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		// For IN and NOT IN operators, check InValues instead of Operand2
		if c.Operator == In || c.Operator == NotIn {
			if len(c.InValues) == 0 {
				return fmt.Errorf("at WHERE: IN/NOT IN condition without values")
			}
		} else {
			if c.Operand2 == "" && c.Operand2IsField {
				return fmt.Errorf("at WHERE: condition with empty right side operand")
			}
		}
	}
	if p.query.Type == Insert && len(p.query.Inserts) == 0 {
		return fmt.Errorf("at INSERT INTO: need at least one row to insert")
	}
	if p.query.Type == Insert {
		for _, i := range p.query.Inserts {
			if len(i) != len(p.query.Fields) {
				return fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
		}
	}
	return nil
}

func (p *parser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}

func isIdentifier(s string) bool {
	for _, rw := range reservedWords {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
	return matched
}

func isIdentifierOrAsterisk(s string) bool {
	return isIdentifier(s) || s == "*"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// FilterRecursive applies a SQL query to a map of data and returns a filtered map using recursion.
// The data is expected to be a map where the key is a unique identifier (like an ID)
// and the value is another map representing a row, with column names as keys and values of type any.
func FilterRecursive(sql string, data map[string]map[string]any) (map[string]map[string]any, error) {
	q, err := Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if q.Type != Select {
		return nil, fmt.Errorf("only SELECT queries can be filtered")
	}

	filteredData := make(map[string]map[string]any)

	// Recursively filter each row
	for key, row := range data {
		if evaluateConditionsRecursive(row, q.Conditions, 0) {
			filteredData[key] = row
		}
	}

	return filteredData, nil
}

// evaluateConditionsRecursive recursively evaluates all conditions using AND logic
// conditionIndex represents the current condition being evaluated
func evaluateConditionsRecursive(row map[string]any, conditions []Condition, conditionIndex int) bool {
	// Base case: if we've evaluated all conditions successfully, return true
	if conditionIndex >= len(conditions) {
		return true
	}

	// Evaluate the current condition
	currentCondition := conditions[conditionIndex]
	if !evaluateConditionRecursive(row, currentCondition) {
		// If current condition fails, short-circuit and return false
		return false
	}

	// Recursively evaluate the next condition
	return evaluateConditionsRecursive(row, conditions, conditionIndex+1)
}

// evaluateConditionRecursive recursively evaluates a single condition
func evaluateConditionRecursive(row map[string]any, cond Condition) bool {
	// Get the field value using recursive field access
	value, exists := getFieldValueRecursive(row, strings.Split(cond.Operand1, "."), 0)
	if !exists {
		return false
	}

	// Handle different operators recursively
	return evaluateOperatorRecursive(value, cond)
}

// getFieldValueRecursive recursively accesses nested fields using dot notation
// fieldParts contains the field path split by dots, partIndex is the current part being accessed
func getFieldValueRecursive(data map[string]any, fieldParts []string, partIndex int) (any, bool) {
	// Base case: we've reached the final field part
	if partIndex >= len(fieldParts) {
		return nil, false
	}

	currentPart := fieldParts[partIndex]
	value, exists := data[currentPart]
	if !exists {
		return nil, false
	}

	// Base case: this is the last part, return the value
	if partIndex == len(fieldParts)-1 {
		return value, true
	}

	// Recursive case: continue with nested map
	nestedMap, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}

	return getFieldValueRecursive(nestedMap, fieldParts, partIndex+1)
}

// evaluateOperatorRecursive recursively evaluates different operators
func evaluateOperatorRecursive(value any, cond Condition) bool {
	switch cond.Operator {
	case Eq:
		return compareValuesRecursive(value, cond.Operand2, "eq")
	case Ne:
		return !compareValuesRecursive(value, cond.Operand2, "eq")
	case Gt:
		return compareValuesRecursive(value, cond.Operand2, "gt")
	case Gte:
		return compareValuesRecursive(value, cond.Operand2, "gte")
	case Lt:
		return compareValuesRecursive(value, cond.Operand2, "lt")
	case Lte:
		return compareValuesRecursive(value, cond.Operand2, "lte")
	case Like:
		return evaluateLikeRecursive(value, cond.Operand2)
	case NotLike:
		return !evaluateLikeRecursive(value, cond.Operand2)
	case In:
		return evaluateInRecursive(value, cond.InValues, 0)
	case NotIn:
		return !evaluateInRecursive(value, cond.InValues, 0)
	default:
		return false
	}
}

// compareValuesRecursive recursively compares two values based on operation type
func compareValuesRecursive(value any, operand2 string, operation string) bool {
	// Try numeric comparison first
	if numResult, ok := compareNumericRecursive(value, operand2, operation); ok {
		return numResult
	}

	// Fall back to string comparison
	return compareStringRecursive(fmt.Sprintf("%v", value), operand2, operation)
}

// compareNumericRecursive attempts numeric comparison recursively
func compareNumericRecursive(value any, operand2 string, operation string) (bool, bool) {
	// Try to convert value to float64
	var numValue float64
	var err error

	switch v := value.(type) {
	case int:
		numValue = float64(v)
	case int64:
		numValue = float64(v)
	case float32:
		numValue = float64(v)
	case float64:
		numValue = v
	case string:
		numValue, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return false, false // Not a number
		}
	default:
		return false, false // Cannot convert to number
	}

	// Try to convert operand2 to float64
	numOperand2, err := strconv.ParseFloat(operand2, 64)
	if err != nil {
		return false, false // operand2 is not a number
	}

	// Perform numeric comparison recursively
	return performNumericComparisonRecursive(numValue, numOperand2, operation), true
}

// performNumericComparisonRecursive performs the actual numeric comparison
func performNumericComparisonRecursive(val1, val2 float64, operation string) bool {
	switch operation {
	case "eq":
		return val1 == val2
	case "gt":
		return val1 > val2
	case "gte":
		return val1 >= val2
	case "lt":
		return val1 < val2
	case "lte":
		return val1 <= val2
	default:
		return false
	}
}

// compareStringRecursive performs string comparison recursively
func compareStringRecursive(val1, val2 string, operation string) bool {
	switch operation {
	case "eq":
		return val1 == val2
	case "gt":
		return val1 > val2
	case "gte":
		return val1 >= val2
	case "lt":
		return val1 < val2
	case "lte":
		return val1 <= val2
	default:
		return false
	}
}

// evaluateLikeRecursive recursively evaluates LIKE pattern matching
func evaluateLikeRecursive(value any, pattern string) bool {
	stringValue, ok := value.(string)
	if !ok {
		return false
	}

	// Convert SQL LIKE pattern to regex recursively
	regexPattern := convertLikePatternRecursive(pattern, 0, "")
	matched, err := regexp.MatchString("^"+regexPattern+"$", stringValue)
	if err != nil {
		return false
	}
	return matched
}

// convertLikePatternRecursive recursively converts SQL LIKE pattern to regex
func convertLikePatternRecursive(pattern string, index int, result string) string {
	// Base case: we've processed the entire pattern
	if index >= len(pattern) {
		return result
	}

	currentChar := pattern[index]
	switch currentChar {
	case '%':
		// % matches zero or more characters
		return convertLikePatternRecursive(pattern, index+1, result+".*")
	case '_':
		// _ matches exactly one character
		return convertLikePatternRecursive(pattern, index+1, result+".")
	case '.', '*', '+', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\':
		// Escape special regex characters
		return convertLikePatternRecursive(pattern, index+1, result+"\\"+string(currentChar))
	default:
		// Regular character
		return convertLikePatternRecursive(pattern, index+1, result+string(currentChar))
	}
}

// evaluateInRecursive recursively evaluates IN operator
func evaluateInRecursive(value any, inValues []string, index int) bool {
	// Base case: we've checked all values and found no match
	if index >= len(inValues) {
		return false
	}

	// Check if current value matches
	if fmt.Sprintf("%v", value) == inValues[index] {
		return true
	}

	// Recursively check the next value
	return evaluateInRecursive(value, inValues, index+1)
}
