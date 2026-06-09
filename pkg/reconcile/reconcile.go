package reconcile

import (
	"bytes"
	"fmt"
	"io"
	"time"
	"log/slog"
	"path/filepath"

	"nomad-gitops-operator/pkg/nomad"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/util"
)

const metaKey = "nomoporator"

type ReconcileOptions struct {
	Path    string
	Watch   bool
	Delete  bool
	Fs      func() (billy.Filesystem, error)
}

func Run(opts ReconcileOptions) error {
	// Create Nomad client
	client, err := nomad.NewClient()
	if err != nil {
		slog.Error("error creating Nomad client", "error", err)
	}

	// Reconcile
	for true {
		fs, err := opts.Fs()
		if err != nil {
			return err
		}

		slog.Info("globbing Nomad file", "path", opts.Path)
		nomadJobFiles, err := util.Glob(fs, opts.Path)
		if err != nil {
			return err
		}

		desiredStateJobs := make(map[string]interface{})

		// Parse and apply all jobs from within the git repo
		for _, filePath := range nomadJobFiles {
			slog.Info("reading Nomad file", "path", filePath)
			f, err := fs.Open(filePath)
			if err != nil {
				return err
			}
			defer f.Close()

			b, err := io.ReadAll(f)
			if err != nil {
				return err
			}

			// Search for vars
			dirPath := filepath.Dir(filePath)
			d, err := fs.Chroot(dirPath)
			if err != nil {
				return err
			}

			varFiles, err := util.Glob(d, "*.vars")
			if err != nil {
				return err
			}
			if len(varFiles) > 1 {
				return fmt.Errorf("only one var file is supported, got more in %q", dirPath)
			}

			// ParseJob is dependant on the disk layout; especially for system using `file` to load they configuration.
			baseDir := filepath.Join(fs.Root(), dirPath)

			varContent := bytes.NewBuffer([]byte{})
			for i, varFile := range varFiles {
				varFiles[i] = filepath.Join(d.Root(), varFiles[i])

				f, err := d.Open(varFile)
				if err != nil {
					return err
				}
				defer f.Close()

				// Loading the content of the varFiles into a buffer.
				_, err = io.Copy(varContent, f)
				if err != nil {
					return err
				}
			}

			job, err := client.ParseJob(b, baseDir, filepath.Join(d.Root(), filePath), varFiles)
			if err != nil {
				// If a parse error occurs we skip the job an continue with the next job
				slog.Error("failed to parse", "path", filePath, "error", err)
				continue
			}

			_, ok := desiredStateJobs[*job.Name]
			if ok {
				slog.Info("skipping duplicate job", "job", *job.Name, "path", filePath)
				continue
			}

			desiredStateJobs[*job.Name] = job

			// Apply job
			slog.Info("applying job", "job", *job.Name, "path", filePath)
			_, err = client.ApplyJob(job, b, varContent.Bytes())
			if err != nil {
				return err
			}
		}

		// List all jobs managed by Nomoporator
		currentStateJobs, err := client.ListJobs()
		if err != nil {
			slog.Error("failed to list jobs", "error", err)
		}

		// Check if job has the required metadata
		// Check if job is one of the parsed jobs
		for _, job := range currentStateJobs {
			meta := job.Meta

			if _, isManaged := meta[metaKey]; isManaged {
				// If the job is managed by Nomoporator and is part of the desired state
				if _, inDesiredState := desiredStateJobs[*job.Name]; inDesiredState {

				} else {
					if opts.Delete {
						slog.Info("deleting job", "job", *job.Name)
						err = client.DeleteJob(job)
						if err != nil {
							fmt.Println(err)
						}
					}
				}
			}
		}

		if !opts.Watch {
			return nil
		}

		time.Sleep(30 * time.Second)
	}

	return nil
}
