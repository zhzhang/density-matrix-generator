USAGE="usage: corpus_path targets [-h] [-s n] -- generate density matrices

where:
    -h  show this help text
    -j  number of threads to use (default: 1)
    -o  output file (default: matrices/)"

CORPUS_PATH="$1"
TARGETS="$2"
shift 2
NUM_THREADS=1
OUTPUT="matrices"

while [[ $# > 0 ]]
do
key="$1"
case $key in
    -h|--help)
    echo "$USAGE"
    exit
    ;;
    -j|--jobs)
    NUM_THREADS="$2"
    shift 2
    ;;
    -o|--output)
    OUTPUT="$2"
    shift 2
    ;;
esac
done

java -cp build/libs/density-matrix-generator.jar dmatrix.DependencyDMatrixGenerator\
  $CORPUS_PATH $TARGETS $NUM_THREADS $OUTPUT

