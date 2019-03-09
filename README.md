# fromS3-div6-toS3
execute trigger:<br/>
aws s3 put evet<br/>
notify to aws lambda python 3.6 program

in:<br/>
read *TXT file from S3 bascket<br/>

transaction:<br/>
1.each 50000 records load to pandas dataframe<br/>
2.check specific column value(at sample code, column name = "c006") and divide 6 csv files

out:<br/>
write csv file on specified s3 bascket
