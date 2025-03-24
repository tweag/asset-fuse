def _bazel_downloader_fallback_impl(repository_ctx):
    mounter = repository_ctx.which("asset-fuse")
    if mounter == None:
        fail("asset-fuse needs to be installed in the PATH")
    repository_ctx.watch(repository_ctx.attr.manifest)
    dump_result = repository_ctx.execute([mounter, "manifest", "dump", "--format=bazel_download", "--manifest", repository_ctx.attr.manifest])
    if dump_result.return_code != 0:
        fail("Generating rctx.download args from manifest return non-zero code {}: {}".format(dump_result.return_code, dump_result.stderr))
    download_arg_list = json.decode(dump_result.stdout)
    has_build_file = False
    for download_entry in download_arg_list:
        if download_entry["output"] in ["BUILD", "BUILD.bazel"]:
            has_build_file = True
        repository_ctx.download(**download_entry)
    if not has_build_file:
        repository_ctx.file("BUILD.bazel", content = "# generated by asset-fuse fallback using Bazel's downloader\nexports_files(glob([\"**\"]))\n")

_bazel_downloader_fallback_attrs = {
    "manifest": attr.label(
        doc = "The asset-fuse manifest JSON file.",
        mandatory = True,
    ),
}

bazel_downloader_fallback = repository_rule(
    implementation = _bazel_downloader_fallback_impl,
    attrs = _bazel_downloader_fallback_attrs,
)
