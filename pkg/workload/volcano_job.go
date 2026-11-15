package workload

import (
	"context"

	"github.com/binbinpeng123/blitz/pkg/dao"
	"github.com/binbinpeng123/blitz/pkg/utils"

	volcanoapis "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

// BuildVolcanoJob 构建完整的 VolcanoJob（公开 API）
func BuildVolcanoJob(ctx context.Context, job *dao.JobStatus, jobData *dao.JobData, extraData utils.BuildJobExtraData) (*volcanoapis.Job, error) {
	return &volcanoapis.Job{}, nil
}
