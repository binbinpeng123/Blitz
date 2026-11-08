package dao

import (
	"strings"
	"testing"
)

// normSQL 折叠空白，避免模板里的空 group/order/having 占位导致断言脆弱。
func normSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func TestFieldsFormat(t *testing.T) {
	tests := []struct {
		fields string
		want   string
	}{
		{"job_id", "job_id ="},
		{"col_rank__gt", "col_rank >"},
		{"col_rank__ge", "col_rank >="},
		{"col_rank__le", "col_rank <="},
		{"col_rank__lt", "col_rank <"},
		{"job_id__eq", "job_id ="},
		{"job_id__ne", "job_id !="},
		{"job_id__in", "job_id in"},
		{"job_id__not_in", "job_id not in"},
		{"deleted_at__is_null", "deleted_at is null"},
		{"deleted_at__not_null", "deleted_at is not null"},
		{"job_name__icontains", "job_name like"},
	}
	for _, tt := range tests {
		t.Run(tt.fields, func(t *testing.T) {
			if got := fieldsFormat(tt.fields); got != tt.want {
				t.Fatalf("fieldsFormat(%q) = %q, want %q", tt.fields, got, tt.want)
			}
		})
	}
}

func TestValueFormat(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"string", "j1", ` "j1" `},
		{"string slice", []string{"a", "b"}, ` ("a","b") `},
		{"int", 42, " 42 "},
		{"int slice", []int{1, 2}, "(1,2)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := valueFormat(tt.value); got != tt.want {
				t.Fatalf("valueFormat(%v) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestOriginalSQL(t *testing.T) {
	tests := []struct {
		name     string
		where    map[string]interface{}
		table    string
		limit    int
		offset   int
		selected []string
		want     string
	}{
		{
			name:  "empty where",
			where: map[string]interface{}{},
			table: "job_status",
			want:  "select * from job_status",
		},
		{
			name:  "string equality",
			where: map[string]interface{}{"job_id": "j1"},
			table: "job_status",
			want:  `select * from job_status where job_id = "j1"`,
		},
		{
			name:   "limit offset",
			where:  map[string]interface{}{"job_id": "j1"},
			table:  "job_status",
			limit:  10,
			offset: 5,
			want:   `select * from job_status where job_id = "j1" limit 10 offset 5`,
		},
		{
			name:     "selected columns",
			where:    map[string]interface{}{"job_id": "j1"},
			table:    "job_status",
			selected: []string{"id", "job_id"},
			want:     `select id,job_id from job_status where job_id = "j1"`,
		},
		{
			name:  "order by",
			where: map[string]interface{}{"job_id__order_by": "desc"},
			table: "job_status",
			want:  "select * from job_status order by job_id desc",
		},
		{
			name:  "group by and having",
			where: map[string]interface{}{"job_id__group_by": "", "having": "count(*) > 1"},
			table: "job_status",
			want:  "select * from job_status group by job_id having count(*) > 1",
		},
		{
			name:  "is null",
			where: map[string]interface{}{"deleted_at__is_null": 1},
			table: "job_status",
			want:  "select * from job_status where deleted_at is null",
		},
		{
			name:  "not null",
			where: map[string]interface{}{"deleted_at__not_null": 1},
			table: "job_status",
			want:  "select * from job_status where deleted_at is not null",
		},
		{
			name:  "greater than",
			where: map[string]interface{}{"col_rank__gt": 3},
			table: "job_status",
			want:  "select * from job_status where col_rank > 3",
		},
		{
			name:  "in int slice",
			where: map[string]interface{}{"id__in": []int{1, 2}},
			table: "job_status",
			want:  "select * from job_status where id in (1,2)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OriginalSQL(tt.where, tt.table, tt.limit, tt.offset, tt.selected...)
			if normSQL(got) != tt.want {
				t.Fatalf("OriginalSQL() = %q, want %q", normSQL(got), tt.want)
			}
		})
	}
}
