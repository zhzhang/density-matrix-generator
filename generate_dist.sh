USAGE="usage: corpus_path targets [-h] [-s n] -- generator density matrices

where:
    -h  show this help text
    -d  dimension of matrices (default: 2000)
    -j  number of threads to use (default: 1)
    -o  output file (default: matrices/)
    -v  generate and output vector representation"

CORPUS_PATH="$1"
TARGETS="$2"
shift 2
NUM_THREADS=1
DIM=2000
OUTPUT="matrices"
VECTORS=0
WORDMAP=0

while [[ $# > 0 ]]
do
key="$1"
case $key in
    -h|--help)
    echo "$USAGE"
    exit
    ;;
    -d|--dim)
    DIM="$2"
    shift 2
    ;;
    -j|--jobs)
    NUM_THREADS="$2"
    shift 2
    ;;
    -o|--output)
    OUTPUT="$2"
    shift 2
    ;;
    -v|--vectors)
    VECTORS=1
    shift
    ;;
    -w|--wordmap)
    WORDMAP=1
    shift
    ;;
esac
done

java -cp build/libs/density-matrix-generator.jar dmatrix.DistributionalDMatrixGenerator\
  $CORPUS_PATH $TARGETS $DIM $NUM_THREADS $OUTPUT $VECTORS $WORDMAP
