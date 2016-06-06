USAGE="usage: corpus_path targets embeddings [-h] [-s n] -- generator density matrices

where:
    -h  show this help text
    -n  number of context words to use (default: 4000)
    -j  number of threads to use (default: 1)
    -p  context level normalization (default: false)
    -o  output file (default: matrices/)"

CORPUS_PATH="$1"
TARGETS="$2"
VECTORS="$3"
shift 2
NUM_THREADS=1
N=4000
PRENORM=0
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
    N="$2"
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
    -p|--prenorm)
    PRENORM=1
    ;;
esac
shift
done

java -cp build/libs/density-matrix-generator.jar dmatrix.EmbeddingDMatrixGenerator\
  $CORPUS_PATH $TARGETS $N $VECTORS $NUM_THREADS $PRENORM $OUTPUT

