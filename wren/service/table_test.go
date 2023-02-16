package service

//TODO: re-implment this method using the modern fenl schema
/*
func TestGetSchemaViolations(t *testing.T) {
	t.Run("should produce list of no more than 6 violations", func(t *testing.T) {
		g := NewGomegaWithT(t)
		tableSchema := "Schema struct {\n  Id *string\n  Num *float64\n  Steps *float64\n  Timezone *int32\n  Time *string\n  Session *string\n  Screen *string\n  Date_ts *int64\n  Transaction *string\n  Score *bool\n  Column *float64\n}"
		fileSchema := "Schema struct {\n  Id *string\n  Completed *float64\n  Num *float64\n  Timezone *int32\n  Time *int32\n  Session *int32\n  Screen *string\n  Date_ts *int64\n  Transaction *string\n  Score *int32\n  Purchase *float64\n}"
		violations := getSchemaViolations(tableSchema, fileSchema)
		g.Expect(violations).Should(ConsistOf(
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "different_column_type", Description: "column: 'Session' has type: '*string' in the table and '*int32' in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "different_column_type", Description: "column: 'Score' has type: '*bool' in the table and '*int32' in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "missing_column", Description: "column: 'Column' with type: '*float64' is missing in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "missing_column", Description: "column: 'Steps' with type: '*float64' is missing in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "different_column_type", Description: "column: 'Time' has type: '*string' in the table and '*int32' in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "additional_issues", Description: "2 additional schema issues exist in the file. these were dropped from the return to ensure the response does not exceed the maximum error response size. fix the issues above and retry to see additional violations."}),
		))
	})

	t.Run("should not use `additional_issues` when there is only one additional issue.", func(t *testing.T) {
		g := NewGomegaWithT(t)
		tableSchema := "Schema struct {\n  Id *string\n  Num *float64\n  Steps *float64\n  Timezone *int32\n  Time *string\n  Session *string\n  Screen *string\n  Date_ts *int64\n  Transaction *string\n  Score *bool\n  Column *float64\n}"
		fileSchema := "Schema struct {\n  Id *string\n  Completed *float64\n  Num *float64\n  Timezone *int32\n  Time *int32\n  Session *string\n  Screen *string\n  Date_ts *int64\n  Transaction *string\n  Score *int32\n  Purchase *float64\n}"
		violations := getSchemaViolations(tableSchema, fileSchema)
		g.Expect(violations).Should(ConsistOf(
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "different_column_type", Description: "column: 'Time' has type: '*string' in the table and '*int32' in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "different_column_type", Description: "column: 'Score' has type: '*bool' in the table and '*int32' in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "missing_column", Description: "column: 'Column' with type: '*float64' is missing in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "missing_column", Description: "column: 'Steps' with type: '*float64' is missing in the file"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "extra_column", Description: "column: 'Completed' with type: '*float64' does not exist in the table"}),
			BeEquivalentTo(&customerrors.Violation{Kind: "schema_violation", Subject: "extra_column", Description: "column: 'Purchase' with type: '*float64' does not exist in the table"}),
		))
	})

	t.Run("should be empty when the schemas are equivalent", func(t *testing.T) {
		g := NewGomegaWithT(t)
		tableSchema := "Schema struct {\n  Id *string\n  Num *float64\n  Steps *float64\n  Timezone *int32\n  Time *string\n  Session *string\n  Screen *string\n  Date_ts *int64\n  Transaction *string\n  Score *bool\n  Column *float64\n}"
		fileSchema := "Schema struct {\n  Id *string\n  Num *float64\n  Steps *float64\n    Timezone *int32\n  Time *string\n      Session *string\n  Screen *string\n   Date_ts *int64\n  Transaction *string\n  Score *bool\n  Column *float64\n}"
		violations := getSchemaViolations(tableSchema, fileSchema)
		g.Expect(violations).Should(HaveLen(0))
	})
}
*/
