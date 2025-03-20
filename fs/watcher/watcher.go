package watcher

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	goFUSEfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/fs"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
	"github.com/tweag/asset-fuse/service/prefetcher"
)

// ManifestWatcher watches a manifest file for changes and updates the view accordingly.
type ManifestWatcher struct {
	manifestPath   string
	manifestDigest integrity.Digest
	manifestMtime  time.Time
	manifestTree   *manifest.ManifestTree
	fsRoot         updateableRoot
	view           manifest.View
	checksumCache  *integrity.ChecksumCache
	digestFunction integrity.Algorithm
	notifyWatcher  *fsnotify.Watcher
	closeOnce      sync.Once
}

// New creates a new ManifestWatcher.
func New(view manifest.View, config api.GlobalConfig, checksumCache *integrity.ChecksumCache, prefetcher *prefetcher.Prefetcher) (*ManifestWatcher, goFUSEfs.InodeEmbedder, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, err
	}

	digestFunction, _ := integrity.AlgorithmFromString(config.DigestFunction)
	rawManifest, err := os.ReadFile(config.ManifestPath)
	if err != nil {
		return nil, nil, err
	}
	initialManifestDigest, err := digestFunction.CalculateDigest(bytes.NewReader(rawManifest))
	if err != nil {
		return nil, nil, err
	}
	initialManifest, err := manifest.TreeFromManifest(bytes.NewReader(rawManifest), view, digestFunction)
	if err != nil {
		return nil, nil, err
	}
	for _, leaf := range initialManifest.Leafs {
		// Try to prefill the checksum cache with the checksums from the initial manifest.
		if checksum, ok := leaf.Integrity.ChecksumForAlgorithm(digestFunction); ok && leaf.SizeHint >= 0 {
			digest := integrity.NewDigest(checksum.Hash, leaf.SizeHint, digestFunction)
			checksumCache.PutIntegrity(leaf.Integrity, digest)
		}
	}
	var failReads bool
	if config.FailReads != nil {
		failReads = *config.FailReads
	}
	root := fs.Root(initialManifest, digestFunction, time.Now(), config.DigestXattrName, fs.XattrEncodingFromString(config.DigestXattrEncoding), failReads, prefetcher)

	return &ManifestWatcher{
		manifestPath:   config.ManifestPath,
		manifestDigest: initialManifestDigest,
		manifestMtime:  time.Now(),
		manifestTree:   &initialManifest,
		fsRoot:         root,
		view:           view,
		checksumCache:  checksumCache,
		digestFunction: digestFunction,
		notifyWatcher:  watcher,
	}, root, nil
}

// Start starts the ManifestWatcher.
func (w *ManifestWatcher) Start(ctx context.Context, wg *sync.WaitGroup) error {
	logging.Basicf("Starting watcher for %s (%v)", w.manifestPath, w.manifestDigest.Hex(w.digestFunction))
	manifestAbsPath, err := filepath.Abs(w.manifestPath)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer w.Stop()
		defer logging.Basicf("Stopped manifest watcher")
		for {
			select {
			case event, ok := <-w.notifyWatcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) && event.Name == manifestAbsPath {
					logging.Debugf("manifest file might have changed")
					if err := w.updateFilesystemTreeOnChange(); err != nil {
						logging.Errorf("error updating tree: %v", err)
					}
				}
			case err, ok := <-w.notifyWatcher.Errors:
				if !ok {
					return
				}
				logging.Errorf("manifest watcher encountered error: %v", err)
			case <-ctx.Done():
				return // context cancelled, call stop in defer
			}
		}
	}()

	if err := w.notifyWatcher.Add(filepath.Dir(manifestAbsPath)); err != nil {
		return err
	}
	return nil
}

// Stop stops the ManifestWatcher.
func (w *ManifestWatcher) Stop() (closeErr error) {
	w.closeOnce.Do(func() {
		closeErr = w.notifyWatcher.Close()
	})
	return closeErr
}

func (w *ManifestWatcher) updateFilesystemTreeOnChange() error {
	newManifestTree, shouldUpdate, err := w.reloadManifestTreeIfChanged()
	if err != nil {
		return err
	}
	if !shouldUpdate {
		return nil
	}

	logging.Basicf("manifest was changed, updating tree (%v)", w.manifestDigest.Hex(w.digestFunction))

	for _, leaf := range newManifestTree.Leafs {
		// Prefill the checksum cache with the checksums from the updated manifest.
		if checksum, ok := leaf.Integrity.ChecksumForAlgorithm(w.digestFunction); ok && leaf.SizeHint >= 0 {
			digest := integrity.NewDigest(checksum.Hash, leaf.SizeHint, w.digestFunction)
			w.checksumCache.PutIntegrity(leaf.Integrity, digest)
		}
	}

	w.fsRoot.UpdateManifest(newManifestTree.Root)
	w.fsRoot.UpdateMtime(w.manifestMtime)

	// TODO: check if this is sufficient
	// Likely, a tree walk is needed to update the tree in place.
	oldManifestTree := w.manifestTree
	w.manifestTree = newManifestTree

	// notify kernel that the contents of the root directory have changed
	for name := range oldManifestTree.Root.Children {
		if _, ok := oldManifestTree.Root.Children[name]; !ok {
			w.fsRoot.NotifyDelete(name, nil)
		} else {
			w.fsRoot.NotifyEntry(name)
		}
	}
	return nil
}

func (w *ManifestWatcher) reloadManifestTreeIfChanged() (newTree *manifest.ManifestTree, shouldUpdate bool, err error) {
	manifestFile, err := os.Open(w.manifestPath)
	if err != nil {
		return nil, false, err
	}
	defer manifestFile.Close()
	info, err := manifestFile.Stat()
	if err != nil {
		return nil, false, err
	}
	mtime := info.ModTime()

	var contents bytes.Buffer
	digestReader := io.TeeReader(manifestFile, &contents)
	newDigest, err := w.digestFunction.CalculateDigest(digestReader)
	if err != nil {
		return nil, false, err
	}

	if newDigest == w.manifestDigest {
		logging.Debugf("manifest digest is the same, skipping update")
		return nil, false, nil
	}

	var syntaxErr manifest.ManifestDecodeError
	tree, err := manifest.TreeFromManifest(&contents, w.view, w.digestFunction)
	if errors.As(err, &syntaxErr) {
		logging.Warningf("syntax error in manifest - skipping update: %v", err)
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}

	w.manifestDigest = newDigest
	w.manifestMtime = mtime
	return &tree, true, nil
}

type updatableDirectory interface {
	UpdateManifest(manifestNode *manifest.Directory)
	NotifyEntry(name string) syscall.Errno
	NotifyDelete(name string, child *goFUSEfs.Inode) syscall.Errno
}

type updateableRoot interface {
	updatableDirectory
	UpdateMtime(mtime time.Time)
}

type updatableLeaf interface {
	UpdateManifest(manifestNode *manifest.Leaf)
	NotifyContent(off, sz int64) syscall.Errno
}
