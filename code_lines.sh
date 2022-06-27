find . ! -path "./third_party*" "(" -name "*.cc" -or -name "*.cpp" -or -name "*.h" ")" -print | xargs wc -l
