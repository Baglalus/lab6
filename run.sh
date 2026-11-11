#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
mkdir -p build
# Работает и при наличии модуля компилятора без отдельной команды javac.
java -m jdk.compiler/com.sun.tools.javac.Main -encoding UTF-8 -d build Lab6.java ShowerRoom.java
exec java -cp build Lab6 "$@"
