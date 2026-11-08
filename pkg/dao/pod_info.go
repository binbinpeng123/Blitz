package dao

import (
	"context"
	"time"
)

type PodInfo struct {
	ID         int       `db:"id"`          // 'id'
	JobID      string    `db:"job_id"`      // '任务ID'
	PodName    string    `db:"pod_name"`    // 'pod名称'
	NodeName   string    `db:"node_name"`   // '节点名称'
	RetryRound int       `db:"retry_round"` // '重试轮次'
	CreateTime time.Time `db:"create_time"` // '创建时间'
	UpdateTime time.Time `db:"update_time"` // '更新时间'
	Devices    string    `db:"devices"`     // '板卡信息'
}

func (conn *Dao) InsertPodInfoIfNotExist(ctx context.Context, pod *PodInfo) error {
	if pod == nil {
		return nil
	}

	sql := `INSERT IGNORE INTO pod_info (job_id, pod_name, node_name, retry_round, create_time, update_time) 
              VALUES (:job_id, :pod_name, :node_name, :retry_round, :create_time, :update_time)`
	_, err := conn.DB.NamedExecContext(ctx, sql, pod)

	return err
}
