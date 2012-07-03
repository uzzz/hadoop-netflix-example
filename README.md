# About
This is sample hadoop application that counts overall average rating and first two weeks average rating
after movie release from netflix's prize sample data (not included). It uses Distributed Cache data
join and custom RecordReader and InputFormat

# FileNameLineRecordReader and FileNameTextInputFormat
There is easier way to get file name from Map part, but I left these two classes
as a sample of custom RecordReader and InputFormat. Maybe someone find it useful.
