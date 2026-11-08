package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/binbinpeng123/blitz/pkg"
	jmoironsqlx "github.com/jmoiron/sqlx"
	slardarsqlx "gitlab.hobot.cc/aidi/slardar/sqlx"
)

// containsMatcher 用 "|" 分隔期望片段，命中全部片段即通过。
// OriginalSQL 遍历 map 生成 where，条件顺序不固定，不能用整句精确匹配。
func containsMatcher(expectedSQL, actualSQL string) error {
	normActual := strings.Join(strings.Fields(actualSQL), " ")
	for _, fragment := range strings.Split(expectedSQL, "|") {
		normWant := strings.Join(strings.Fields(fragment), " ")
		if !strings.Contains(normActual, normWant) {
			return fmt.Errorf("sql %q does not contain %q", normActual, normWant)
		}
	}
	return nil
}

func newMockDB(t *testing.T) (*slardarsqlx.DB, sqlmock.Sqlmock) {
	t.Helper()
	sdb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(containsMatcher)))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = sdb.Close() })
	return &slardarsqlx.DB{DB: jmoironsqlx.NewDb(sdb, "mysql").Unsafe()}, mock
}

func TestGetJobStatus(t *testing.T) {
	where := map[string]interface{}{"job_id": "j1"}

	t.Run("query rows and inject soft delete filters", func(t *testing.T) {
		db, mock := newMockDB(t)
		mock.ExpectQuery("select * from job_status where|job_id = \"j1\"|is_deleted = 0|last_record = 0|limit 1 offset 0").
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "job_id", "job_name", "phase", "last_record", "is_deleted",
			}).AddRow(1, "j1", "job-name", pkg.JobPhaseQueuing, "0", "0"))

		dao := &Dao{DB: db}
		jobs, err := dao.GetJobStatus(context.Background(), where, 1, 0)
		if err != nil {
			t.Fatalf("GetJobStatus: %v", err)
		}
		if len(jobs) != 1 {
			t.Fatalf("got %d jobs, want 1", len(jobs))
		}
		if jobs[0].JobID != "j1" || jobs[0].Phase != pkg.JobPhaseQueuing {
			t.Fatalf("unexpected job: %+v", jobs[0])
		}
		// GetJobStatus 会就地修改入参 map，这里固化该行为
		if where["is_deleted"] != 0 || where["last_record"] != 0 {
			t.Fatalf("where not injected: %+v", where)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("query error", func(t *testing.T) {
		db, mock := newMockDB(t)
		mock.ExpectQuery("select * from job_status").WillReturnError(errors.New("db down"))

		dao := &Dao{DB: db}
		if _, err := dao.GetJobStatus(context.Background(), map[string]interface{}{"job_id": "j1"}, 1, 0); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestGenerateJobUpdateSQL(t *testing.T) {
	t.Run("nil data", func(t *testing.T) {
		sql, args := (&Dao{}).generateJobUpdateSQL("j1", nil)
		if sql != "" || args != nil {
			t.Fatalf("got sql=%q args=%+v, want empty", sql, args)
		}
	})

	t.Run("empty data", func(t *testing.T) {
		sql, args := (&Dao{}).generateJobUpdateSQL("j1", &JobUpdateParams{})
		if sql != "" || args != nil {
			t.Fatalf("got sql=%q args=%+v, want empty", sql, args)
		}
	})

	t.Run("phase only rebuilds update time", func(t *testing.T) {
		sql, args := (&Dao{}).generateJobUpdateSQL("j1", &JobUpdateParams{
			Phase:       pkg.JobPhaseRunning,
			BeforePhase: []string{pkg.JobPhaseInitializing, pkg.JobPhaseQueuing},
		})
		want := "update job_status set phase = ?,col_rank = 0,update_time = ? " +
			"where is_deleted = false and last_record = 0 and job_id = ? and phase in (?,?)"
		if normSQL(sql) != want {
			t.Fatalf("sql = %q, want %q", normSQL(sql), want)
		}
		wantArgs := []interface{}{pkg.JobPhaseRunning, sqlmock.AnyArg(), "j1", pkg.JobPhaseInitializing, pkg.JobPhaseQueuing}
		assertArgs(t, args, wantArgs)
	})

	t.Run("queuing phase keeps col_rank", func(t *testing.T) {
		sql, _ := (&Dao{}).generateJobUpdateSQL("j1", &JobUpdateParams{Phase: pkg.JobPhaseQueuing})
		if strings.Contains(sql, "col_rank") {
			t.Fatalf("queuing phase should not reset col_rank: %q", sql)
		}
	})

	t.Run("all fields", func(t *testing.T) {
		submitted := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		running := submitted.Add(time.Minute)
		finish := submitted.Add(2 * time.Minute)
		scheduled := submitted.Add(3 * time.Minute)
		data := &JobUpdateParams{
			Phase:         pkg.JobPhaseRunning,
			BeforePhase:   []string{pkg.JobPhaseInitializing},
			Reason:        "reason",
			StopMessage:   strings.Repeat("a", 100),
			SubmittedTime: submitted,
			RunningTime:   running,
			FinishTime:    finish,
			ScheduledTime: scheduled,
			Priority:      "high",
			LastRecord:    LastRecord,
			FileSystem:    "fs",
			GPUAvgUsage:   0.5,
		}
		sql, args := (&Dao{}).generateJobUpdateSQL("j1", data)
		want := "update job_status set phase = ?,col_rank = 0,reason = ?,stop_message = ?," +
			"submitted_time = ?,running_time = ?,finish_time = ?,scheduled_time = ?," +
			"priority = ?,last_record = ?,file_system = ?,gpu_avg_usage = ?,update_time = ? " +
			"where is_deleted = false and last_record = 0 and job_id = ? and phase in (?)"
		if normSQL(sql) != want {
			t.Fatalf("sql = %q, want %q", normSQL(sql), want)
		}
		if len(args) != 14 {
			t.Fatalf("got %d args, want 14: %+v", len(args), args)
		}
		if args[2] != strings.Repeat("a", 80) {
			t.Fatalf("stop_message not truncated to 80 bytes: %q", args[2])
		}
		// generateJobUpdateSQL 会就地截断入参
		if data.StopMessage != strings.Repeat("a", 80) {
			t.Fatalf("data.StopMessage mutated unexpectedly: %q", data.StopMessage)
		}
	})

	t.Run("last record only when equal to 1", func(t *testing.T) {
		sql, _ := (&Dao{}).generateJobUpdateSQL("j1", &JobUpdateParams{Phase: pkg.JobPhaseRunning, LastRecord: 2})
		if strings.Contains(sql, "last_record = ?") {
			t.Fatalf("last_record should be skipped: %q", sql)
		}
	})

	t.Run("zero gpu usage skipped", func(t *testing.T) {
		sql, _ := (&Dao{}).generateJobUpdateSQL("j1", &JobUpdateParams{Phase: pkg.JobPhaseRunning, GPUAvgUsage: 0})
		if strings.Contains(sql, "gpu_avg_usage") {
			t.Fatalf("zero gpu_avg_usage should be skipped: %q", sql)
		}
	})
}

func TestUpdateJobStatus(t *testing.T) {
	t.Run("exec success with zero rows affected", func(t *testing.T) {
		db, mock := newMockDB(t)
		mock.ExpectExec("update job_status set phase = ?,col_rank = 0,update_time = ?").
			WithArgs(pkg.JobPhaseRunning, sqlmock.AnyArg(), "j1").
			WillReturnResult(sqlmock.NewResult(0, 0))

		dao := &Dao{DB: db}
		// Phase 非空但影响行数为 0，不应触发 Kafka 投递
		if err := dao.UpdateJobStatus(context.Background(), "j1", "job-name", &JobUpdateParams{Phase: pkg.JobPhaseRunning}); err != nil {
			t.Fatalf("UpdateJobStatus: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("exec error", func(t *testing.T) {
		db, mock := newMockDB(t)
		mock.ExpectExec("update job_status").WillReturnError(errors.New("db down"))

		dao := &Dao{DB: db}
		if err := dao.UpdateJobStatus(context.Background(), "j1", "job-name", &JobUpdateParams{Phase: pkg.JobPhaseRunning}); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func assertArgs(t *testing.T, got, want []interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d args %+v, want %d args %+v", len(got), got, len(want), want)
	}
	for i := range want {
		// sqlmock.AnyArg 是 mySQLMockArgument 类型，交给 reflect 比较不合适，这里只比较非 AnyArg
		if _, ok := want[i].(sqlmock.Argument); ok {
			continue
		}
		if got[i] != want[i] {
			t.Fatalf("arg[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
