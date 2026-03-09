# make it executable: chmod +x download_data.sh
# execute: ./download_data.sh

# we want the script to quit on the first non-zero code: (could happen if not all the 12 months of data are available)
set -e

TAXI_TYPE=$1 # pass argument to command line instead of TAXI_TYPE="yellow"
YEAR=$2 # pass argument to command line instead of YEAR=2020


# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

# for the months in url we have 01, 02 etc., so we need to format it somehow: 
# format as 02d: 0 means i want to add leading 0, 2 means i want to have 2 digits, d means it's a digit

for MONTH in {1..12}; do
  FMONTH=$(printf "%02d" ${MONTH})

  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "Downloading ${URL} to ${LOCAL_PATH}"
  echo wget ${URL} -O ${LOCAL_PATH}  # -O option in wget tells wget what filename (and path) to save the downloaded file as
  
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

  # gzip ${LOCAL_PATH}  # we could zip the file to save space, and look at it with zcat file | head -n 5, for example, or zcat file | wc -l to count how many rows
  # to take a look at the parquet file: duckdb -c "SELECT * FROM 'yellow_tripdata_2020-01.parquet' LIMIT 5;"
  # tree command: tree data

done