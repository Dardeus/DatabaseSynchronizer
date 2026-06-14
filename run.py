#!/usr/bin/env python3
import subprocess
import sys

def run_docker_compose(*args):
    cmd = ["docker", "compose"] + list(args)
    print(f"Выполняется: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def main():
    if len(sys.argv) < 2:
        print("Использование: python run.py {build|dry-run|sync} [--schema-only]")
        sys.exit(1)

    command = sys.argv[1]
    extra_args = sys.argv[2:]

    if command == "build":
        run_docker_compose("build")
    elif command == "dry-run":
        run_docker_compose("run", "--rm", "sync", "--dry-run", *extra_args)
    elif command == "sync":
        confirm = input("Режим синхронизации! Введите 'yes' для продолжения: ")
        if confirm.lower() == "yes":
            run_docker_compose("run", "--rm", "sync", "--no-dry-run", *extra_args)
    else:
        print(f"Неизвестная команда: {command}")

if __name__ == "__main__":
    main()