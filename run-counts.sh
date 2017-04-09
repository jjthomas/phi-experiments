for size in 8 128 1024 8192 131072 1048576 8388608; do
  for copies in 1 4 64; do
    ./count -n 100663296 -b $size -t 64 -c $copies
  done
done
