package controller

import (
	"net/http"
	"time"

	"github.com/argoproj/argo-workflows/v3/util/env"
	log "github.com/sirupsen/logrus"
)

var (
	age = env.LookupEnvDurationOr("HEALTHZ_AGE", 5*time.Minute)
)

// https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-http-request
// If we are in a state where there are any workflows that have not been reconciled in the last 2m, we've gone wrong.
func (ctrl *Controller) Healthz(w http.ResponseWriter, r *http.Request) {
	err := func() error {
		// TODO
		// ctrl.volcanoClient.BatchV1alpha1().Jobs().List(r.Context())

		// for _, wf := range list.Items {
		// 	if time.Since(wf.GetCreationTimestamp().Time) > age {
		// 		return fmt.Errorf("vj never reconciled: %s", wf.Name)
		// 	}
		// }
		return nil
	}()
	log.WithField("err", err).
		WithField("age", age).
		Info("healthz")
	if err != nil {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(err.Error()))
	} else {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}
}
