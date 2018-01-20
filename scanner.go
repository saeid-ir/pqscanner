package pqscanenr

import (
	"database/sql"
	"encoding/json"
	"reflect"
)

// Costume scanner type
type Scanner struct {
	*sql.DB
}

// New return the costume scanner instance
func New(db *sql.DB) *Scanner {
	return &Scanner{db}
}

// row2mapInterface get rows and fields name and return array of interface
func row2mapInterface(rows *sql.Rows, fields []string) (resultsMap map[string]interface{}, err error) {
	resultsMap = make(map[string]interface{}, len(fields))
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		resultsMap[key] = reflect.Indirect(reflect.ValueOf(scanResultContainers[ii])).Interface()
	}
	return
}

// rows2Interfaces return the the entire query result as interface with the given query rows
func rows2Interfaces(rows *sql.Rows) (resultsSlice []map[string]interface{}, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := row2mapInterface(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}
	return resultsSlice, nil
}

// QueryInterface return the query result as interface
func (scanner *Scanner) QueryInterface(sqlStr string, args ...interface{}) ([]map[string]interface{}, error) {
	var rows *sql.Rows
	var err error
	rows, err = scanner.Query(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	return rows2Interfaces(rows)
}

func (scanner *Scanner) QueryJson(sqlStr string, args ...interface{}) (*json.RawMessage, error) {
	var jsonData *json.RawMessage
	var row *sql.Row
	row = scanner.QueryRow(sqlStr, args...)
	err := row.Scan(&jsonData)
	if err != nil {
		return nil, err
	}
	return jsonData, nil
}
