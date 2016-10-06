A = LOAD 'ToPigInput' AS (count:int, word:chararray);
B = ORDER A by count DESC;
C = LIMIT B 10;
DUMP C;
