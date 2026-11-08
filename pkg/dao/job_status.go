package dao

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/binbinpeng123/blitz/client"
	"github.com/binbinpeng123/blitz/pkg"
)

const LastRecord = 1

type JobStatus struct {
	ID              int64      `db:"id"`                           // 'id'
	JobID           string     `db:"job_id"`                       // '任务ID'
	JobName         string     `db:"job_name"`                     // '任务名称'
	Reason          string     `db:"reason"`                       // '停止原因'
	StopMessage     string     `db:"stop_message"`                 // '错误消息'
	Phase           string     `db:"phase"`                        // '状态'
	JobSubmitedTime time.Time  `db:"job_submited_time"`            // '用户提交job时间，不可变'
	CreateTime      time.Time  `db:"create_time"`                  // '创建时间'
	UpdateTime      time.Time  `db:"update_time"`                  // '更新时间'
	SubmittedTime   *time.Time `db:"submitted_time"`               // 'k8s提交时间'
	ScheduledTime   *time.Time `db:"scheduled_time"`               // '调度完成时间'
	RunningTime     *time.Time `db:"running_time"`                 // '运行时间'
	FinishTime      *time.Time `db:"finish_time"`                  // '完成时间'
	Resource        string     `db:"resource"`                     // 'pod资源规格(多容器累加)'
	ColRank         int        `db:"col_rank"`                     // '排位: 加急任务写入时间戳'
	LastRecord      int        `db:"last_record"`                  // '最后一次任务记录，retry后置为1'
	IsDeleted       int        `db:"is_deleted"`                   // '0未删除,1删除'
	DAGName         string     `db:"dag_name"`                     // 'dag名字'
	DAGID           string     `db:"dag_id"`                       // 'dagID'
	Namespace       string     `db:"namespace"`                    // '命名空间/队列'
	UserName        string     `db:"user_name"`                    // '用户名'
	JobType         string     `db:"job_type"`                     // '任务类型'
	Priority        string     `db:"priority"`                     // '任务优先级'
	ProjectID       string     `db:"project_id"`                   // '项目ID'
	JobRunMaxTime   int        `db:"job_max_running_time_minutes"` // '运行时长/min'
	FileSystem      string     `db:"file_system"`                  // '任务挂载的文件系统'
	K8S             string     `db:"k8s"`                          // '任务所属的集群'
	Unit            string     `db:"unit"`                         // '任务所属的单元'
	RetryUser       string     `db:"retry_user"`                   // 'retry用户'
	GpuAvgUsage     float32    `db:"gpu_avg_usage"`                // 'GPU平均利用率'
}

func (conn *Dao) GetJobStatus(ctx context.Context, where map[string]interface{}, limit, offset int, selected ...string) ([]*JobStatus, error) {
	where["is_deleted"] = 0
	where["last_record"] = 0
	sqlStr := OriginalSQL(where, "job_status", limit, offset)
	data := make([]*JobStatus, 0)
	if err := conn.DB.SelectContext(ctx, &data, sqlStr); err != nil {
		return nil, err
	}

	return data, nil
}

type JobUpdateParams struct {
	Phase         string
	BeforePhase   []string
	Reason        string
	StopMessage   string
	SubmittedTime time.Time
	RunningTime   time.Time
	FinishTime    time.Time
	ScheduledTime time.Time
	Priority      string
	LastRecord    int
	FileSystem    string
	GPUAvgUsage   float32
}

func (conn *Dao) UpdateJobStatus(ctx context.Context, jobId, jobName string, data *JobUpdateParams) error {
	sql, args := conn.generateJobUpdateSQL(jobId, data)
	result, err := conn.DB.ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 && err == nil && data.Phase != "" {
		status := client.KafkaStatus{
			Id:     jobId,
			Name:   jobName,
			Status: data.Phase,
			Reason: data.Reason,
		}
		go client.Produce(status, client.JobStatusTopic)
	}

	return err
}

func (conn *Dao) generateJobUpdateSQL(jobId string, data *JobUpdateParams) (sql string, args []interface{}) {
	if data == nil {
		return
	}

	var sets []string
	if data.Phase != "" {
		sets = append(sets, "phase = ?")
		args = append(args, data.Phase)

		if data.Phase != pkg.JobPhaseQueuing {
			sets = append(sets, "col_rank = 0")
		}
	}
	if data.Reason != "" {
		sets = append(sets, "reason = ?")
		args = append(args, data.Reason)
	}
	if data.StopMessage != "" {
		// 极限：中文3字符*80 < 255
		if len(data.StopMessage) > 80 {
			data.StopMessage = data.StopMessage[:80]
		}
		sets = append(sets, "stop_message = ?")
		args = append(args, data.StopMessage)
	}
	if !data.SubmittedTime.IsZero() {
		sets = append(sets, "submitted_time = ?")
		args = append(args, data.SubmittedTime)
	}
	if !data.RunningTime.IsZero() {
		sets = append(sets, "running_time = ?")
		args = append(args, data.RunningTime)
	}
	if !data.FinishTime.IsZero() {
		sets = append(sets, "finish_time = ?")
		args = append(args, data.FinishTime)
	}
	if !data.ScheduledTime.IsZero() {
		sets = append(sets, "scheduled_time = ?")
		args = append(args, data.ScheduledTime)
	}
	if data.Priority != "" {
		sets = append(sets, "priority = ?")
		args = append(args, data.Priority)
	}
	if data.LastRecord == LastRecord {
		sets = append(sets, "last_record = ?")
		args = append(args, data.LastRecord)
	}
	if data.FileSystem != "" {
		sets = append(sets, "file_system = ?")
		args = append(args, data.FileSystem)
	}
	if data.GPUAvgUsage != 0 {
		sets = append(sets, "gpu_avg_usage = ?")
		args = append(args, data.GPUAvgUsage)
	}
	if len(sets) == 0 {
		return
	}
	sets = append(sets, "update_time = ?")
	args = append(args, time.Now())

	sql = "update job_status set %s where is_deleted = false and last_record = 0 and job_id = ? "
	args = append(args, jobId)
	if len(data.BeforePhase) != 0 {
		in := ""
		for i := 0; i < len(data.BeforePhase); i++ {
			in += "?"
			if i < len(data.BeforePhase)-1 {
				in += ","
			}
			args = append(args, data.BeforePhase[i])
		}

		sql += fmt.Sprintf("and phase in (%s)", in)
	}
	sql = fmt.Sprintf(sql, strings.Join(sets, ","))

	return
}
