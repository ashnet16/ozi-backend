import os
import subprocess


def compile_proto():
    proto_dir = "proto"
    proto_files = [f for f in os.listdir(proto_dir) if f.endswith(".proto")]

    if not proto_files:
        print("No .proto files found in 'proto/'.")
        return

    for file in proto_files:
        path = os.path.join(proto_dir, file)
        print(f"Compiling {path}...")

        result = subprocess.run(
            [
                "python",
                "-m",
                "grpc_tools.protoc",
                f"-I{proto_dir}",
                f"--python_out={proto_dir}",
                f"--grpc_python_out={proto_dir}",
                path,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print(f"Failed to compile {file}")
            print(result.stderr)
        else:
            print(f"Compiled {file}")


if __name__ == "__main__":
    compile_proto()
