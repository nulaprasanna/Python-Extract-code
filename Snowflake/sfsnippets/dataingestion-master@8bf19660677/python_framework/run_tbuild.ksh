#!/bin/ksh
echo "Inside tbuild" > /tmp/test_log
v_TBL_SIZE=${1}
echo "Table $TARGET_TABLE_NAME is of size $v_TBL_SIZE" >> /tmp/size_log
if [ $v_TBL_SIZE -lt 1048576 ] ## table < 1 GB
then
 TEMPLATE=Tbuild_template_Micro.txt
elif [ $v_TBL_SIZE -lt 104857600 ] ## table < 100 GB
then
 TEMPLATE=Tbuild_template_Mini.txt
else
 TEMPLATE=Tbuild_template_Medium.txt
fi
SCR_DIR=/apps/edwsfdata/talend_DIY/scripts/shell
#sed -e 's/TBLNAME/'"${TARGET_TABLE_NAME}"'/g' ${SCR_DIR}/Tbuild_template.txt > ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}_1.txt
sed -e 's/TBLNAME/'"${TARGET_TABLE_NAME}"'/g' ${SCR_DIR}/${TEMPLATE} > ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}_1.txt
WHERE_LENGTH=$(echo "$WHERE_CLAUSE" | wc -c)
REV_WHERE_CLAUSE=$(echo "$WHERE_CLAUSE" | sed -e "s#'#''#g")
## Invoke script to include trim for char/varchar fields
SOURCE_QUERY=$(${SCR_DIR}/query_mod.ksh)
echo "SOURCEQUERY is $SOURCE_QUERY" >> /tmp/test_log
## Remove last comma from source query
SRCQRY=$(echo $SOURCE_QUERY | rev | cut -c 2- | rev | sed -e "s#'#''#g")
BQRY=$(echo "$SRCQRY FROM ${SOURCE_SCHEMA}.${SOURCE_TABLE_NAME}" | sed -e "s#''#'#g") 
if [ "$WHERE_CLAUSE" = " WHERE " ]
then
 if [ "$EXTRACT_TYPE" = "DATE" ]
 then
  REV_WHERE_CLAUSE="${WHERE_CLAUSE} ${INCREMENTAL_COLUMN_NAME} > to_date(''${TO_EXTRACT_DTM}'',''YYYY/MM/DD HH24:MI:SS'')"
 elif [ "$EXTRACT_TYPE" = "ID" ]
 then
  REV_WHERE_CLAUSE="${WHERE_CLAUSE} ${INCREMENTAL_COLUMN_NAME} > ${TO_EXTRACT_ID}"
 fi
 REVSRC=$(echo "${SRCQRY} FROM ${SOURCE_SCHEMA}.${SOURCE_TABLE_NAME} ${REV_WHERE_CLAUSE}")
elif [ "$WHERE_LENGTH" -gt 7 ]
then
  REVSRC=$(echo "${SRCQRY} FROM ${SOURCE_SCHEMA}.${SOURCE_TABLE_NAME} ${REV_WHERE_CLAUSE}")
else
  REVSRC=$(echo "${SRCQRY} FROM ${SOURCE_SCHEMA}.${SOURCE_TABLE_NAME}")
fi
sed -e 's#TFNAME#'"${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME}"'#g' ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}_1.txt > ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}_2.txt
sed -e 's/TDELIMITER/'"${SRC2STG_FF_DELIMITER}"'/g' ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}_2.txt > ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}.txt
echo ",SelectStmt     = '$REVSRC'" >> ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}.txt
tbuild -f ${SCR_DIR}/jobdef.txt -v ${CONFIG_PATH}/jobvars_${TARGET_TABLE_NAME}_${BATCH_ID}.txt -j ${TARGET_TABLE_NAME}_${BATCH_ID}
if [ $? -ne 0 ]
then
  echo "${TARGET_TABLE_NAME} - tbuild failed.. Hence aborting"
  exit 1
fi
## Change to check if generated file is zero byte file
if [ -s ${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME} ]
then
 ${SCR_DIR}/bteq_col_head.ksh "$BQRY" ${SRC2STG_FF_PATH} ${BATCH_ID} ${SCR_DIR}
 if [ $? -ne 0 ]
 then
   echo "${TARGET_TABLE_NAME} - bteq failed.. Hence aborting"
   exit 1
 fi
 ## Extract header from bteq spool
 #HEADER=$(head -1 ${SRC2STG_FF_PATH}/bteq_${BATCH_ID}.out | sed -e "s/ //g")
 head -1 ${SRC2STG_FF_PATH}/bteq_${BATCH_ID}.out | sed -e "s/ //g" > ${SRC2STG_FF_PATH}/bteq_header_${BATCH_ID}
 ## Append header as first line in data file
 #sed -i '1s/^/'"$HEADER"'\n/' ${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME}
 ### Below change done on 12-Oct-2019 since cat was a costly timeconsuming operation
 #cat ${SRC2STG_FF_PATH}/bteq_header_${BATCH_ID} ${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME} > ${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME}.rev
 #mv ${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME}.rev ${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME}
 /apps/edwsfdata/python/scripts/appender_header.py  -f ${SRC2STG_FF_PATH}/bteq_header_${BATCH_ID} -d ${SRC2STG_FF_PATH}/${CURRENT_SRC2STG_FF_NAME}
 ## End of change done on 12-Oct-2019
 rm -f ${SRC2STG_FF_PATH}/bteq_header_${BATCH_ID}
else
 echo "Zero byte data file detected.."
fi
rm -f  ${SRC2STG_FF_PATH}/bteq_${BATCH_ID}.out  ${SRC2STG_FF_PATH}/bteq_${BATCH_ID}.log
