package dao

import (
	"fmt"
	"strings"
)

// OriginalSQL original sql join
// where: >=|>|<=|<|=|!=|is null|not null|in
// selected：* ｜ 字段 ｜ 聚合查询
// limit | offset  default: offset=0
func OriginalSQL(where map[string]interface{}, tableName string, limit, offset int, selected ...string) string {
	selectStr := "*"
	if len(selected) > 0 {
		selectStr = strings.Join(selected, ",")
	}

	var whereStr string
	var order, group string
	var having string
	for fields, value := range where {
		if strings.HasSuffix(fields, "__order_by") {
			order = fmt.Sprintf("order by %s %s", strings.Replace(fields, "__order_by", "", -1), value)
			continue
		}
		if strings.HasSuffix(fields, "__group_by") {
			group = fmt.Sprintf("group by %s", strings.Replace(fields, "__group_by", "", -1))
			continue
		}

		if hg := havingFormat(fields, value); hg != "" {
			having = hg
			continue
		}
		if whereStr != "" {
			whereStr += " and "
		}
		// is_null / not_null
		if strings.HasSuffix(fields, "__is_null") || strings.HasSuffix(fields, "__not_null") {
			whereStr += fieldsFormat(fields)
			continue
		}

		whereStr += fmt.Sprintf("%s %s", fieldsFormat(fields), valueFormat(value))
	}

	if whereStr != "" {
		whereStr = "where " + whereStr
	}

	if limit > 0 {
		return fmt.Sprintf("select %s from %s %s %s %s %s limit %d offset %d",
			selectStr, tableName, whereStr, group, order, having, limit, offset)
	}
	return fmt.Sprintf("select %s from %s %s %s %s %s", selectStr, tableName, whereStr, group, order, having)
}

func havingFormat(fields string, value interface{}) string {
	if fields == "having" {
		return fmt.Sprintf("having %v", value)
	}
	return ""
}

func fieldsFormat(fields string) string {
	if strings.HasSuffix(fields, "__gt") {
		return strings.Replace(fields, "__gt", " >", -1)
	}
	if strings.HasSuffix(fields, "__ge") {
		return strings.Replace(fields, "__ge", " >=", -1)
	}
	if strings.HasSuffix(fields, "__le") {
		return strings.Replace(fields, "__le", " <=", -1)
	}
	if strings.HasSuffix(fields, "__lt") {
		return strings.Replace(fields, "__lt", " <", -1)
	}
	if strings.HasSuffix(fields, "__eq") {
		return strings.Replace(fields, "__eq", " =", -1)
	}
	if strings.HasSuffix(fields, "__ne") {
		return strings.Replace(fields, "__ne", " !=", -1)
	}
	if strings.HasSuffix(fields, "__in") {
		return strings.Replace(fields, "__in", " in", -1)
	}
	if strings.HasSuffix(fields, "__not_in") {
		return strings.Replace(fields, "__not_in", " not in", -1)
	}
	if strings.HasSuffix(fields, "__is_null") {
		return strings.Replace(fields, "__is_null", " is null", -1)
	}
	if strings.HasSuffix(fields, "__not_null") {
		return strings.Replace(fields, "__not_null", " is not null", -1)
	}
	if strings.HasSuffix(fields, "__icontains") {
		return strings.Replace(fields, "__icontains", " like", -1)
	}
	return fmt.Sprintf("%s =", fields)
}

func valueFormat(value interface{}) string {
	if _, ok := value.(string); ok {
		return fmt.Sprintf(" \"%s\" ", value)
	}

	if d, ok := value.([]string); ok {
		return fmt.Sprintf(" (\"%s\") ", strings.Join(d, "\",\""))
	}

	s := fmt.Sprintf("%v", value)
	if strings.Contains(s, "[") && strings.Contains(s, "]") {
		return strings.Replace(strings.Replace(strings.Replace(s, "[", "(", -1), "]", ")", -1), " ", ",", -1)
	}

	return fmt.Sprintf(" %v ", value)
}
