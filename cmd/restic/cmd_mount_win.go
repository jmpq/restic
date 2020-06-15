// +build windows

package main

import (
	cgofuse "github.com/billziss-gh/cgofuse/fuse"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fuse"
	"github.com/restic/restic/internal/restic"
	"github.com/spf13/cobra"
)

var cmdMount = &cobra.Command{
	Use:   "mount [flags] mountpoint",
	Short: "Mount the repository",
	Long: `
The "mount" command mounts the repository via fuse to a directory. This is a
read-only mount.

Snapshot Directories
====================

If you need a different template for all directories that contain snapshots,
you can pass a template via --snapshot-template. Example without colons:

    --snapshot-template "2006-01-02_15-04-05"

You need to specify a sample format for exactly the following timestamp:

    Mon Jan 2 15:04:05 -0700 MST 2006

For details please see the documentation for time.Format() at:
  https://godoc.org/time#Time.Format

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runMount(mountOptions, globalOptions, args)
	},
}

// MountOptions collects all options for the mount command.
type MountOptions struct {
	OwnerRoot            bool
	AllowOther           bool
	NoDefaultPermissions bool
	Hosts                []string
	Tags                 restic.TagLists
	Paths                []string
	SnapshotTemplate     string
}

var mountOptions MountOptions

func init() {
	cmdRoot.AddCommand(cmdMount)

	mountFlags := cmdMount.Flags()
	mountFlags.BoolVar(&mountOptions.OwnerRoot, "owner-root", false, "use 'root' as the owner of files and dirs")
	mountFlags.BoolVar(&mountOptions.AllowOther, "allow-other", false, "allow other users to access the data in the mounted directory")
	mountFlags.BoolVar(&mountOptions.NoDefaultPermissions, "no-default-permissions", false, "for 'allow-other', ignore Unix permissions and allow users to read all snapshot files")

	mountFlags.StringArrayVarP(&mountOptions.Hosts, "host", "H", nil, `only consider snapshots for this host (can be specified multiple times)`)
	mountFlags.Var(&mountOptions.Tags, "tag", "only consider snapshots which include this `taglist`")
	mountFlags.StringArrayVar(&mountOptions.Paths, "path", nil, "only consider snapshots which include this (absolute) `path`")

	mountFlags.StringVar(&mountOptions.SnapshotTemplate, "snapshot-template", "2006-01-02_15-04-05", "set `template` to use for snapshot dirs")

}

func mount(opts MountOptions, gopts GlobalOptions, args []string) error {

	debug.Log("start mount")
	defer debug.Log("finish mount")

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	lock, err := lockRepo(repo)
	defer unlockRepo(lock)
	if err != nil {
		return err
	}

	err = repo.LoadIndex(gopts.ctx)
	if err != nil {
		return err
	}

	cfg := fuse.Config{
		OwnerIsRoot:      opts.OwnerRoot,
		Hosts:            opts.Hosts,
		Tags:             opts.Tags,
		Paths:            opts.Paths,
		SnapshotTemplate: opts.SnapshotTemplate,
	}

	fs, err := fuse.NewWinFs(gopts.ctx, repo, cfg)
	if err != nil {
		return err
	}

	Printf("Now serving the repository at %s\n", args[0])
	Printf("When finished, quit with Ctrl-c or umount the mountpoint.\n")

	host := cgofuse.NewFileSystemHost(fs)

	host.SetCapReaddirPlus(true)
	host.Mount("", args)

	return nil
}

func runMount(opts MountOptions, gopts GlobalOptions, args []string) error {
	if len(args) == 0 {
		return errors.Fatal("wrong number of parameters")
	}

	return mount(opts, gopts, args)
}
