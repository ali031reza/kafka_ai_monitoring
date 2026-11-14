# DeepStream model artifacts

Keep model weights, exported ONNX files, and generated TensorRT engines in
this directory at development/deployment time. The files are intentionally
excluded from Git because they may be large, hardware-specific, or subject
to separate model licensing.

Suggested layout:

    deepstream/models/
      person_face/
        model.onnx
        model.engine
        labels.txt

The paths are examples only. Record model version, input preprocessing,
output format, TensorRT precision, and engine build environment in the
corresponding model documentation.

Do not commit camera credentials, private model weights, or generated
engines. Configure the runtime to mount or provision these artifacts.
