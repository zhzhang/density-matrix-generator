USAGE="usage: corpus_path targets [-h] [-s n] -- generator density matrices

where:
    -h  show this help text
    -d  dimension of matrices (default: 2000)
    -v  path to pre-built word vectors
    -j  number of threads to use (default: 1)
    -o  output file (default: ./matrices.dat)
    -s  path to stopwords file"

CORPUS_PATH="$1"
TARGETS="$2"
shift 2
NUM_THREADS=1
DIM=2000
N=4000
STOPWORDS="stopwords.txt"
OUTPUT="matrices.dat"

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
    shift
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
    -s|--stopwords)
    STOPWORDS="$2"
    shift
    ;;
    -v|--vectors)
    VECTORS="$2"
    shift
    ;;
esac
shift
done

if [ ! -z $VECTORS ]; then
  java -cp build/libs/density-matrix-generator.jar dmatrix.DMatrixGeneratorDense\
    $CORPUS_PATH $TARGETS $N $VECTORS $NUM_THREADS $STOPWORDS $OUTPUT
else
  java -cp build/libs/density-matrix-generator.jar dmatrix.DMatrixGenerator \
    $CORPUS_PATH $TARGETS $DIM $NUM_THREADS $STOPWORDS $OUTPUT
fi

