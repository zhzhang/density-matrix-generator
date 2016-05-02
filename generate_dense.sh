USAGE="usage: corpus_path targets [-h] [-s n] -- generator density matrices

where:
    -h  show this help text
    -n  number of context words to use (default: 4000)
    -v  path to pre-built word vectors
    -j  number of threads to use (default: 1)
    -o  output file (default: matrices/)"

CORPUS_PATH="$1"
TARGETS="$2"
shift 2
NUM_THREADS=1
N=4000
OUTPUT="matrices"

while [[ $# > 0 ]]
do
key="$1"
case $key in
    -h|--help)
    echo "$USAGE"
    exit
    ;;
    -n|--num_vectors)
    DIM="$2"
    shift
    ;;
    -j|--jobs)
    NUM_THREADS="$2"
    shift
    ;;
    -o|--output)
    OUTPUT="$2"
    shift
    ;;
    -v|--vectors)
    VECTORS="$2"
    shift
    ;;
esac
shift
done

java -cp build/libs/density-matrix-generator.jar dmatrix.DMatrixGeneratorDense\
  $CORPUS_PATH $TARGETS $N $VECTORS $NUM_THREADS $OUTPUT

