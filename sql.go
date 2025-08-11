package sqlparser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/hootrhino/sqlparser/query"
)

// Parse takes a string representing a SQL query and parses it into a query.Query struct. It may fail.
func Parse(sqls string) (query.Query, error) {
	qs, err := ParseMany([]string{sqls})
	if len(qs) == 0 {
		return query.Query{}, err
	}
	return qs[0], err
}

// ParseMany takes a string slice representing many SQL queries and parses them into a query.Query struct slice.
// It may fail. If it fails, it will stop at the first failure.
func ParseMany(sqls []string) ([]query.Query, error) {
	qs := []query.Query{}
	for _, sql := range sqls {
		q, err := parse(sql)
		if err != nil {
			return qs, err
		}
		qs = append(qs, q)
	}
	return qs, nil
}

func parse(sql string) (query.Query, error) {
	return (&parser{0, strings.TrimSpace(sql), stepType, query.Query{}, nil, ""}).parse()
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
	query           query.Query
	err             error
	nextUpdateField string
}

func (p *parser) parse() (query.Query, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) doParse() (query.Query, error) {
	for {
		if p.i >= len(p.sql) {
			return p.query, p.err
		}
		switch p.step {
		case stepType:
			QType := strings.ToUpper(p.peek())
			switch QType {
			case "SELECT":
				p.query.Type = query.Select
				p.pop()
				p.step = stepSelectField
			case "INSERT INTO":
				p.query.Type = query.Insert
				p.pop()
				p.step = stepInsertTable
			case "UPDATE":
				p.query.Type = query.Update
				p.query.Updates = map[string]string{}
				p.pop()
				p.step = stepUpdateTable
			case "DELETE FROM":
				p.query.Type = query.Delete
				p.pop()
				p.step = stepDeleteFromTable
			case "CREATE TABLE":
				p.query.Type = query.Create
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
			p.query.Conditions = append(p.query.Conditions, query.Condition{Operand1: identifier, Operand1IsField: true})
			p.pop()
			p.step = stepWhereOperator
		case stepWhereOperator:
			operator := p.peek()
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			switch operator {
			case "=":
				currentCondition.Operator = query.Eq
			case ">":
				currentCondition.Operator = query.Gt
			case ">=":
				currentCondition.Operator = query.Gte
			case "<":
				currentCondition.Operator = query.Lt
			case "<=":
				currentCondition.Operator = query.Lte
			case "!=":
				currentCondition.Operator = query.Ne
			case "LIKE":
				currentCondition.Operator = query.Like
			case "NOT LIKE":
				currentCondition.Operator = query.NotLike
			case "IN":
				currentCondition.Operator = query.In
			case "NOT IN":
				currentCondition.Operator = query.NotIn
			default:
				return p.query, fmt.Errorf("at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()

			// For IN and NOT IN operators, expect opening parenthesis
			if currentCondition.Operator == query.In || currentCondition.Operator == query.NotIn {
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
			if currentCondition.Operator == query.Like || currentCondition.Operator == query.NotLike {
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
	if p.query.Type == query.UnknownType {
		return fmt.Errorf("query type cannot be empty")
	}
	if p.query.Type == query.Create {
		return nil
	}
	if p.query.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(p.query.Conditions) == 0 && (p.query.Type == query.Update || p.query.Type == query.Delete) {
		return fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == query.UnknownOperator {
			return fmt.Errorf("at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		// For IN and NOT IN operators, check InValues instead of Operand2
		if c.Operator == query.In || c.Operator == query.NotIn {
			if len(c.InValues) == 0 {
				return fmt.Errorf("at WHERE: IN/NOT IN condition without values")
			}
		} else {
			if c.Operand2 == "" && c.Operand2IsField {
				return fmt.Errorf("at WHERE: condition with empty right side operand")
			}
		}
	}
	if p.query.Type == query.Insert && len(p.query.Inserts) == 0 {
		return fmt.Errorf("at INSERT INTO: need at least one row to insert")
	}
	if p.query.Type == query.Insert {
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

// Filter applies a SQL query to a map of data and returns a filtered map.
// The data is expected to be a map where the key is a unique identifier (like an ID)
// and the value is another map representing a row, with column names as keys and values of type any.
func Filter(sql string, data map[string]map[string]any) (map[string]map[string]any, error) {
	q, err := Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if q.Type != query.Select {
		return nil, fmt.Errorf("only SELECT queries can be filtered")
	}

	filteredData := make(map[string]map[string]any)

	for key, row := range data {
		if checkConditions(row, q.Conditions) {
			filteredData[key] = row
		}
	}

	return filteredData, nil
}

func checkConditions(row map[string]any, conditions []query.Condition) bool {
	if len(conditions) == 0 {
		return true
	}

	for _, cond := range conditions {
		if !checkCondition(row, cond) {
			return false
		}
	}

	return true
}

func checkCondition(row map[string]any, cond query.Condition) bool {
	// Handle nested field access using a dot notation, e.g., "user.address.city"
	field := cond.Operand1
	fieldParts := strings.Split(field, ".")

	var value any
	currentMap := row
	var exists bool
	for i, part := range fieldParts {
		if i == len(fieldParts)-1 {
			value, exists = currentMap[part]
		} else {
			nestedValue, ok := currentMap[part]
			if !ok {
				exists = false
				break
			}
			currentMap, ok = nestedValue.(map[string]any)
			if !ok {
				exists = false
				break
			}
		}
	}

	if !exists {
		// If the field doesn't exist, the condition is not met
		return false
	}

	// Operand2 is always a string from the parser, so we need to convert the value from the map to a comparable type.
	// For simplicity, we'll try to convert both to a string for comparison, but we could also handle numbers, etc.
	// We'll perform type-safe comparisons for different operators.
	switch cond.Operator {
	case query.Eq:
		return fmt.Sprintf("%v", value) == cond.Operand2
	case query.Ne:
		return fmt.Sprintf("%v", value) != cond.Operand2
	case query.Gt:
		return fmt.Sprintf("%v", value) > cond.Operand2
	case query.Gte:
		return fmt.Sprintf("%v", value) >= cond.Operand2
	case query.Lt:
		return fmt.Sprintf("%v", value) < cond.Operand2
	case query.Lte:
		return fmt.Sprintf("%v", value) <= cond.Operand2
	case query.Like:
		stringValue, ok := value.(string)
		if !ok {
			return false
		}
		pattern := strings.ReplaceAll(cond.Operand2, "%", ".*")
		pattern = strings.ReplaceAll(pattern, "_", ".")
		matched, _ := regexp.MatchString("^"+pattern+"$", stringValue)
		return matched
	case query.NotLike:
		stringValue, ok := value.(string)
		if !ok {
			return false
		}
		pattern := strings.ReplaceAll(cond.Operand2, "%", ".*")
		pattern = strings.ReplaceAll(pattern, "_", ".")
		matched, _ := regexp.MatchString("^"+pattern+"$", stringValue)
		return !matched
	case query.In:
		for _, inValue := range cond.InValues {
			if fmt.Sprintf("%v", value) == inValue {
				return true
			}
		}
		return false
	case query.NotIn:
		for _, inValue := range cond.InValues {
			if fmt.Sprintf("%v", value) == inValue {
				return false
			}
		}
		return true
	default:
		// Unknown operator, assume condition is not met
		return false
	}
}
