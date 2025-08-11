package query

import (
	"fmt"
	"strings"
)

// Query represents a parsed query
type Query struct {
	Type         Type
	TableName    string
	Conditions   []Condition
	Updates      map[string]string
	Inserts      [][]string
	Fields       []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases      map[string]string
	CreateFields map[string]string // name1 type, name2 type ...
}

func (q Query) String() string {
	var sb strings.Builder

	switch q.Type {
	case Select:
		sb.WriteString("SELECT ")
		if len(q.Fields) > 0 {
			for i, field := range q.Fields {
				sb.WriteString(field)
				if alias, ok := q.Aliases[field]; ok {
					sb.WriteString(" AS ")
					sb.WriteString(alias)
				}
				if i < len(q.Fields)-1 {
					sb.WriteString(", ")
				}
			}
		} else {
			sb.WriteString("*")
		}
		sb.WriteString(" FROM ")
		sb.WriteString(q.TableName)
	case Insert:
		sb.WriteString("INSERT INTO ")
		sb.WriteString(q.TableName)
		sb.WriteString(" (")
		sb.WriteString(strings.Join(q.Fields, ", "))
		sb.WriteString(") VALUES ")
		for i, row := range q.Inserts {
			sb.WriteString("('")
			sb.WriteString(strings.Join(row, "', '"))
			sb.WriteString("')")
			if i < len(q.Inserts)-1 {
				sb.WriteString(", ")
			}
		}
	case Update:
		sb.WriteString("UPDATE ")
		sb.WriteString(q.TableName)
		sb.WriteString(" SET ")
		i := 0
		for field, value := range q.Updates {
			sb.WriteString(field)
			sb.WriteString(" = '")
			sb.WriteString(value)
			sb.WriteString("'")
			if i < len(q.Updates)-1 {
				sb.WriteString(", ")
			}
			i++
		}
	case Delete:
		sb.WriteString("DELETE FROM ")
		sb.WriteString(q.TableName)
	case Create:
		sb.WriteString("CREATE TABLE ")
		sb.WriteString(q.TableName)
		sb.WriteString(" (")
		i := 0
		for field, fieldType := range q.CreateFields {
			sb.WriteString(field)
			sb.WriteString(" ")
			sb.WriteString(fieldType)
			if i < len(q.CreateFields)-1 {
				sb.WriteString(", ")
			}
			i++
		}
		sb.WriteString(")")
	default:
		return ""
	}

	if len(q.Conditions) > 0 {
		sb.WriteString(" WHERE ")
		for i, cond := range q.Conditions {
			if cond.Operand1IsField {
				sb.WriteString(cond.Operand1)
			} else {
				sb.WriteString(fmt.Sprintf("'%s'", cond.Operand1))
			}
			sb.WriteString(" ")
			sb.WriteString(cond.Operator.String())
			sb.WriteString(" ")

			if cond.Operator == In || cond.Operator == NotIn {
				sb.WriteString("('")
				sb.WriteString(strings.Join(cond.InValues, "', '"))
				sb.WriteString("')")
			} else {
				if cond.Operand2IsField {
					sb.WriteString(cond.Operand2)
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", cond.Operand2))
				}
			}

			if i < len(q.Conditions)-1 {
				sb.WriteString(" AND ")
			}
		}
	}

	return sb.String()
}

// Type is the type of SQL query, e.g. SELECT/UPDATE
type Type int

const (
	// UnknownType is the zero value for a Type
	UnknownType Type = iota
	// Select represents a SELECT query
	Select
	// Update represents an UPDATE query
	Update
	// Insert represents an INSERT query
	Insert
	// Delete represents a DELETE query
	Delete
	// Create
	Create
)

// TypeString is a string slice with the names of all types in order
var TypeString = []string{
	"UnknownType",
	"Select",
	"Update",
	"Insert",
	"Delete",
	"Create",
}

// Operator is between operands in a condition
type Operator int

func (i Operator) String() string {
	switch i {
	case Eq:
		return "="
	case Ne:
		return "!="
	case Gt:
		return ">"
	case Lt:
		return "<"
	case Gte:
		return ">="
	case Lte:
		return "<="
	case Like:
		return "LIKE"
	case NotLike:
		return "NOT LIKE"
	case In:
		return "IN"
	case NotIn:
		return "NOT IN"
	default:
		return "UnknownOperator"
	}
}

const (
	// UnknownOperator is the zero value for an Operator
	UnknownOperator Operator = iota
	// Eq -> "="
	Eq
	// Ne -> "!="
	Ne
	// Gt -> ">"
	Gt
	// Lt -> "<"
	Lt
	// Gte -> ">="
	Gte
	// Lte -> "<="
	Lte
	// Like -> "LIKE"
	Like
	// NotLike -> "NOT LIKE"
	NotLike
	// In -> "IN"
	In
	// NotIn -> "NOT IN"
	NotIn
)

// OperatorString is a string slice with the names of all operators in order
var OperatorString = []string{
	"UnknownOperator",
	"Eq",
	"Ne",
	"Gt",
	"Lt",
	"Gte",
	"Lte",
	"Like",
	"NotLike",
	"In",
	"NotIn",
}

// Condition is a single boolean condition in a WHERE clause
type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operand1IsField bool
	// Operator is e.g. "=", ">", "LIKE", "IN"
	Operator Operator
	// Operand2 is the right hand side operand (for LIKE, IN this can be a single value or list)
	Operand2 string
	// Operand2IsField determines if Operand2 is a literal or a field name
	Operand2IsField bool
	// InValues holds the list of values for IN operator
	InValues []string
}
