package dao

import (
	"context"
	"time"
)

type JobData struct {
	ID         int64     `db:"id"`          // 'id'
	JobID      string    `db:"job_id"`      // '任务ID'
	IsUrgent   int       `db:"is_urgent"`   // '废弃'
	RetryLimit int       `db:"retry_limit"` // 'retry上限'
	RetryCnt   int       `db:"retry_cnt"`   // 'retry数'
	Request    *string   `db:"request"`     // '请求资源' (使用*string表示可为NULL)
	CreateTime time.Time `db:"create_time"` // '创建时间'
	UpdateTime time.Time `db:"update_time"` // '更新时间'
	PodCnt     int       `db:"pod_cnt"`     // '副本数'
}

func (conn *Dao) GetJobData(ctx context.Context, where map[string]interface{}) ([]*JobData, error) {
	data := make([]*JobData, 0)
	sqlStr := OriginalSQL(where, "job_data", 1, 0)
	if err := conn.DB.SelectContext(ctx, &data, sqlStr); err != nil {
		return nil, err
	}

	return data, nil
}
