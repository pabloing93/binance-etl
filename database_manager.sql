-- Creating user
create user pablo_data_analyst password 'Data1234';

-- Creating group and assigning it a user

CREATE GROUP data_analysis WITH USER pablo_analyst;

-- Grants privilegies to the group

GRANT ALL 
ON schema.table TO data_analysis;


GRANT SELECT(columns, columns) 
ON schema.table 
TO GROUP data_analysis;