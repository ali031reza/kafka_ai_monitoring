"""Check for the GStreamer and NVIDIA plugins used by the first pipeline."""

import json
import sys


REQUIRED_PLUGINS = ("nvstreammux", "nvinfer", "nvv4l2decoder")


def inspect_runtime() -> dict:
    try:
        import gi
        gi.require_version("Gst", "1.0")
        from gi.repository import Gst
    except Exception as error:  # Runtime diagnostics must report missing deps clearly.
        return {
            "ok": False,
            "gstreamer_available": False,
            "plugins": {},
            "error": f"GStreamer Python bindings are unavailable: {error}",
        }

    Gst.init(None)
    plugins = {
        plugin_name: Gst.ElementFactory.find(plugin_name) is not None
        for plugin_name in REQUIRED_PLUGINS
    }
    missing = [name for name, available in plugins.items() if not available]

    return {
        "ok": not missing,
        "gstreamer_available": True,
        "gstreamer_version": Gst.version_string(),
        "plugins": plugins,
        "missing_plugins": missing,
    }


def main() -> int:
    report = inspect_runtime()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
