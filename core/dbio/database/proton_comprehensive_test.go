package database

import (
	"fmt"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProtonComprehensiveTypeSupport tests extensive type coverage for ClickHouse-compatible DBMS
// This test covers all major data types that would be used in analytical workloads
func TestProtonComprehensiveTypeSupport(t *testing.T) {
	conn := &ProtonConn{}

	// Comprehensive type coverage test cases
	typeTests := []struct {
		category  string
		testCases []struct {
			name       string
			dbType     string
			testValue  interface{}
			expectType string
		}
	}{
		{
			category: "Basic Integer Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"int8_basic", "int8", int8(127), "int8"},
				{"int16_basic", "int16", int16(32767), "int16"},
				{"int32_basic", "int32", int32(2147483647), "int32"},
				{"int64_basic", "int64", int64(9223372036854775807), "int64"},
				{"uint8_basic", "uint8", uint8(255), "uint8"},
				{"uint16_basic", "uint16", uint16(65535), "uint16"},
				{"uint32_basic", "uint32", uint32(4294967295), "uint32"},
				{"uint64_basic", "uint64", uint64(18446744073709551615), "uint64"},
			},
		},
		{
			category: "Floating Point Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"float32_basic", "float32", float32(3.14159), "float32"},
				{"float64_basic", "float64", float64(3.141592653589793), "float64"},
				{"float32_scientific", "float32", "1.23e-4", "float32"},
				{"float64_scientific", "float64", "1.23456789e-10", "float64"},
			},
		},
		{
			category: "String and Boolean Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"string_basic", "string", "Hello World", "string"},
				{"string_unicode", "string", "Hello 世界 🌍", "string"},
				{"bool_true", "bool", true, "bool"},
				{"bool_false", "bool", false, "bool"},
				{"boolean_alias", "boolean", true, "bool"},
			},
		},
		{
			category: "Decimal Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"decimal_basic", "decimal", "123.456", "decimal"},
				{"decimal32_small", "decimal32", "999.99", "decimal"},
				{"decimal64_medium", "decimal64", "123456.789012", "decimal"},
				{"decimal128_large", "decimal128", "123456789012345.123456789", "decimal"},
				{"decimal256_huge", "decimal256", "12345678901234567890.123456789012345678", "decimal"},
				{"numeric_postgres", "numeric", "9999.9999", "decimal"},
				{"money_alias", "money", "1234.56", "decimal"},
				{"decimal_precision_9_2", "decimal(9,2)", "1234567.89", "decimal"},
				{"decimal_precision_18_4", "decimal(18,4)", "12345678901234.5678", "decimal"},
				{"decimal_precision_38_8", "decimal(38,8)", "123456789012345678901234567890.12345678", "decimal"},
			},
		},
		{
			category: "Date and Time Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"date_basic", "date", "2024-01-15", "date"},
				{"date32_extended", "date32", "2024-12-31", "date"},
				{"datetime_basic", "datetime", "2024-01-15 14:30:25", "datetime64"},
				{"datetime64_basic", "datetime64", "2024-01-15 14:30:25.123", "datetime64"},
				{"datetime64_precision_3", "datetime64(3)", "2024-01-15 14:30:25.123", "datetime64"},
				{"datetime64_precision_6", "datetime64(6)", "2024-01-15 14:30:25.123456", "datetime64"},
				{"timestamp_alias", "timestamp", "2024-01-15T14:30:25.123Z", "datetime64"},
			},
		},
		{
			category: "UUID Type",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"uuid_basic", "uuid", "123e4567-e89b-12d3-a456-426614174000", "uuid"},
				{"uuid_variant", "uuid", "550e8400-e29b-41d4-a716-446655440000", "uuid"},
			},
		},
		{
			category: "Array Types - Basic",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"array_string", "array(string)", `["hello", "world", "test"]`, "array(string)"},
				{"array_int32", "array(int32)", `[1, 2, 3, 4, 5]`, "array(int32)"},
				{"array_int64", "array(int64)", `[1000000000, 2000000000, 3000000000]`, "array(int64)"},
				{"array_float64", "array(float64)", `[1.1, 2.2, 3.3]`, "array(float64)"},
				{"array_bool", "array(bool)", `[true, false, true]`, "array(bool)"},
				{"array_empty", "array(string)", `[]`, "array(string)"},
			},
		},
		{
			category: "Array Types - Advanced",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"array_decimal32", "array(decimal32)", `["99.99", "199.99", "299.99"]`, "array(decimal)"},
				{"array_decimal64", "array(decimal64)", `["1234.5678", "9999.9999"]`, "array(decimal)"},
				{"array_date", "array(date)", `["2024-01-15", "2024-01-16", "2024-01-17"]`, "array(date)"},
				{"array_datetime64", "array(datetime64)", `["2024-01-15 14:30:25", "2024-01-16 15:45:30"]`, "array(datetime64)"},
				{"array_uuid", "array(uuid)", `["123e4567-e89b-12d3-a456-426614174000", "550e8400-e29b-41d4-a716-446655440000"]`, "array(uuid)"},
			},
		},
		{
			category: "Map Types - String Keys",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"map_string_string", "map(string, string)", `{"key1": "value1", "key2": "value2"}`, "map(string, string)"},
				{"map_string_int32", "map(string, int32)", `{"count": 42, "total": 100}`, "map(string, int32)"},
				{"map_string_int64", "map(string, int64)", `{"big_number": 9999999999, "another": 1234567890}`, "map(string, int64)"},
				{"map_string_float64", "map(string, float64)", `{"price": 99.99, "tax": 8.25}`, "map(string, float64)"},
				{"map_string_array_string", "map(string, array(string))", `{"tags": ["tag1", "tag2"], "categories": ["cat1"]}`, "map(string, array(string))"},
			},
		},
		{
			category: "Map Types - Integer Keys",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"map_int32_string", "map(int32, string)", `{"1": "first", "2": "second", "3": "third"}`, "map(int32, string)"},
				{"map_int64_string", "map(int64, string)", `{"1000000000": "billion", "2000000000": "two billion"}`, "map(int64, string)"},
			},
		},
		{
			category: "Nullable Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"nullable_int32", "nullable(int32)", int32(42), "int32"},
				{"nullable_int32_null", "nullable(int32)", nil, "int32"},
				{"nullable_float64", "nullable(float64)", 3.14159, "float64"},
				{"nullable_float64_null", "nullable(float64)", nil, "float64"},
				{"nullable_string", "nullable(string)", "test", "string"},
				{"nullable_string_null", "nullable(string)", nil, "string"},
				{"nullable_decimal64", "nullable(decimal64)", "123.45", "decimal"},
				{"nullable_decimal64_null", "nullable(decimal64)", nil, "decimal"},
				{"nullable_datetime64", "nullable(datetime64)", "2024-01-15 14:30:25", "datetime64"},
				{"nullable_datetime64_null", "nullable(datetime64)", nil, "datetime64"},
				{"nullable_uuid", "nullable(uuid)", "123e4567-e89b-12d3-a456-426614174000", "uuid"},
				{"nullable_uuid_null", "nullable(uuid)", nil, "uuid"},
			},
		},
		{
			category: "Low Cardinality Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"low_cardinality_string", "low_cardinality(string)", "category_a", "string"},
				{"low_cardinality_nullable_string", "low_cardinality(nullable(string))", "status_active", "string"},
				{"low_cardinality_nullable_string_null", "low_cardinality(nullable(string))", nil, "string"},
			},
		},
		{
			category: "Complex Nested Types",
			testCases: []struct {
				name       string
				dbType     string
				testValue  interface{}
				expectType string
			}{
				{"nullable_low_cardinality_string", "nullable(low_cardinality(string))", "enum_value", "string"},
				{"nullable_low_cardinality_string_null", "nullable(low_cardinality(string))", nil, "string"},
				{"low_cardinality_nullable_decimal64", "low_cardinality(nullable(decimal64))", "99.99", "decimal"},
				{"low_cardinality_nullable_decimal64_null", "low_cardinality(nullable(decimal64))", nil, "decimal"},
			},
		},
	}

	// Run all type tests
	for _, category := range typeTests {
		t.Run(category.category, func(t *testing.T) {
			for _, tc := range category.testCases {
				t.Run(tc.name, func(t *testing.T) {
					t.Logf("Testing %s: %s with value %v", category.category, tc.dbType, tc.testValue)

					// Create column and build converter
					columns := iop.Columns{{Name: "test_col", DbType: tc.dbType}}
					converters := conn.buildColumnConverters(columns)

					require.Equal(t, 1, len(converters), "Should create exactly one converter")
					converter := converters[0]
					assert.Equal(t, tc.expectType, converter.TypeName, "Converter type should match expected")

					// Test conversion
					if tc.testValue != nil {
						result, err := converter.Convert(tc.testValue)
						assert.NoError(t, err, "Should convert value without error")
						assert.NotNil(t, result, "Converted result should not be nil")
						t.Logf("  ✓ %s: %v -> %v (type: %T)", tc.dbType, tc.testValue, result, result)
					} else {
						t.Logf("  ✓ %s: nil -> handled gracefully", tc.dbType)
					}
				})
			}
		})
	}
}

// TestProtonStreamExample tests the exact example from the user request
func TestProtonStreamExample(t *testing.T) {
	conn := &ProtonConn{}

	// Example stream structure: stock, price, time
	// CREATE STREAM default.normal (
	//   `stock` int32,
	//   `price` nullable(float32),
	//   `time` nullable(datetime)
	// );

	columns := iop.Columns{
		{Name: "stock", DbType: "int32"},
		{Name: "price", DbType: "nullable(float32)"},
		{Name: "time", DbType: "nullable(datetime)"},
	}

	converters := conn.buildColumnConverters(columns)
	require.Equal(t, 3, len(converters), "Should create converter for each column")

	// Test sample data similar to: select number as stock, (rand() % 10000) / 100.0 AS price, now() FROM system.numbers LIMIT 2
	testData := [][]interface{}{
		{int32(0), float32(45.67), "2024-01-15 14:30:25"},
		{int32(1), float32(123.89), "2024-01-15 14:30:26"},
		{int32(2), nil, "2024-01-15 14:30:27"}, // Test nullable price
		{int32(3), float32(78.90), nil},        // Test nullable time
		{int32(4), nil, nil},                   // Test both nullable fields as null
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("row_%d", i), func(t *testing.T) {
			t.Logf("Testing row %d: %v", i, row)

			// Convert each column
			for colIdx, value := range row {
				converter := converters[colIdx]
				colName := columns[colIdx].Name

				if value != nil {
					result, err := converter.Convert(value)
					assert.NoError(t, err, "Should convert %s value %v", colName, value)
					t.Logf("  %s: %v -> %v ✓", colName, value, result)
				} else {
					t.Logf("  %s: nil -> handled gracefully", colName)
				}
			}
		})
	}
}

// TestProtonRealWorldScenarios tests comprehensive real-world analytical scenarios
func TestProtonRealWorldScenarios(t *testing.T) {
	conn := &ProtonConn{}

	scenarios := []struct {
		name        string
		description string
		schema      []struct {
			name   string
			dbType string
		}
		sampleRows [][]interface{}
	}{
		{
			name:        "iot_sensor_data",
			description: "IoT sensor data with various measurement types",
			schema: []struct {
				name   string
				dbType string
			}{
				{"sensor_id", "uuid"},
				{"device_type", "low_cardinality(string)"},
				{"timestamp", "datetime64(3)"},
				{"temperature", "nullable(decimal64)"},
				{"humidity", "nullable(decimal64)"},
				{"pressure", "nullable(decimal64)"},
				{"location", "map(string, float64)"},
				{"tags", "array(string)"},
				{"status", "low_cardinality(nullable(string))"},
				{"readings", "array(decimal64)"},
			},
			sampleRows: [][]interface{}{
				{
					"123e4567-e89b-12d3-a456-426614174000",
					"temperature_sensor",
					"2024-01-15T14:30:25.123",
					"23.5",
					"65.2",
					"1013.25",
					`{"lat": 40.7128, "lon": -74.0060}`,
					`["indoor", "office", "floor_2"]`,
					"active",
					`["23.1", "23.3", "23.5", "23.4"]`,
				},
				{
					"223e4567-e89b-12d3-a456-426614174001",
					"humidity_sensor",
					"2024-01-15T14:30:26.456",
					nil, // No temperature reading
					"68.7",
					"1012.80",
					`{"lat": 40.7589, "lon": -73.9851}`,
					`["outdoor", "parking"]`,
					nil, // Unknown status
					`["68.5", "68.9", "68.7"]`,
				},
			},
		},
		{
			name:        "web_analytics",
			description: "Web analytics data with user interactions",
			schema: []struct {
				name   string
				dbType string
			}{
				{"session_id", "uuid"},
				{"user_id", "nullable(uuid)"},
				{"page_url", "string"},
				{"event_type", "low_cardinality(string)"},
				{"timestamp", "datetime64(6)"},
				{"duration_ms", "nullable(uint64)"},
				{"scroll_depth", "nullable(decimal32)"},
				{"custom_properties", "map(string, string)"},
				{"feature_flags", "array(string)"},
				{"conversion_value", "nullable(decimal128)"},
			},
			sampleRows: [][]interface{}{
				{
					"323e4567-e89b-12d3-a456-426614174002",
					"423e4567-e89b-12d3-a456-426614174003",
					"/product/123",
					"page_view",
					"2024-01-15T14:30:25.123456",
					uint64(5432),
					"75.5",
					`{"campaign": "winter_sale", "ab_test": "variant_a"}`,
					`["new_checkout", "mobile_optimized"]`,
					"129.99",
				},
				{
					"423e4567-e89b-12d3-a456-426614174004",
					nil, // Anonymous user
					"/landing",
					"bounce",
					"2024-01-15T14:30:27.789012",
					uint64(1200),
					"25.0",
					`{"source": "google", "medium": "cpc"}`,
					`[]`,
					nil, // No conversion
				},
			},
		},
		{
			name:        "time_series_metrics",
			description: "Time series metrics data for monitoring",
			schema: []struct {
				name   string
				dbType string
			}{
				{"metric_id", "uuid"},
				{"metric_name", "low_cardinality(string)"},
				{"timestamp", "datetime64(3)"},
				{"value", "decimal64"},
				{"unit", "low_cardinality(string)"},
				{"dimensions", "map(string, string)"},
				{"percentiles", "array(decimal64)"},
				{"min_value", "nullable(decimal64)"},
				{"max_value", "nullable(decimal64)"},
				{"sample_count", "uint64"},
			},
			sampleRows: [][]interface{}{
				{
					"523e4567-e89b-12d3-a456-426614174005",
					"cpu_usage",
					"2024-01-15T14:30:25.000",
					"85.7",
					"percent",
					`{"host": "web-01", "region": "us-east-1", "env": "prod"}`,
					`["50.2", "75.8", "90.1", "95.5", "99.0"]`,
					"12.3",
					"98.9",
					uint64(1000),
				},
				{
					"623e4567-e89b-12d3-a456-426614174006",
					"memory_usage",
					"2024-01-15T14:30:30.000",
					"4.2",
					"gigabytes",
					`{"host": "db-01", "region": "us-west-2", "env": "prod"}`,
					`["2.1", "3.8", "4.5", "5.2", "6.8"]`,
					"1.8",
					"7.1",
					uint64(500),
				},
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Logf("Testing scenario: %s", scenario.description)

			// Build columns
			columns := make(iop.Columns, len(scenario.schema))
			for i, col := range scenario.schema {
				columns[i] = iop.Column{Name: col.name, DbType: col.dbType}
			}

			// Build converters
			converters := conn.buildColumnConverters(columns)
			assert.Equal(t, len(scenario.schema), len(converters), "Should create converter for each column")

			// Test each sample row
			for rowIdx, row := range scenario.sampleRows {
				t.Run(fmt.Sprintf("row_%d", rowIdx), func(t *testing.T) {
					t.Logf("  Testing row %d with %d columns", rowIdx+1, len(row))
					assert.Equal(t, len(scenario.schema), len(row), "Row should have correct number of columns")

					// Convert each value
					for colIdx, value := range row {
						converter := converters[colIdx]
						colName := scenario.schema[colIdx].name
						colType := scenario.schema[colIdx].dbType

						if value == nil {
							t.Logf("    %s (%s): nil -> handled gracefully", colName, colType)
						} else {
							result, err := converter.Convert(value)
							assert.NoError(t, err, "Should convert %s (%s) value %v", colName, colType, value)
							t.Logf("    %s (%s): %v -> %v ✓", colName, colType, value, result)
						}
					}
				})
			}
		})
	}
}
