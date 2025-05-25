#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include "sqlite.h"
#include <ctype.h>
#include <fcntl.h>

#define MAX_PAGES 100
#define PAGE_SIZE 4096
#define INT_SIZE 4
#define STRING_SIZE 256
#define NEWLINE "\n"

typedef enum {SELECT , INSERT, META, CREATE}StatementType;
typedef enum {EXIT, TABLE}MetaType;
typedef enum {PREPARE_SUCCESS , PREPARE_UNRECOGNIZED}PrepareResult;
typedef enum {META_SUCCESS , META_UNRECOGNIZED}MetaResult;
typedef enum {EXECUTE_SUCCESS, EXECUTE_FAILURE}ExecuteResult;
typedef enum {INT, STRING}DataType;

void token_helper(char*, char*, char*);
void trim(char*);

typedef struct{
	DataType type;
	void* data;
	uint32_t size;
}Data;

typedef struct{
	Data** data;
	int id;
}Row;

typedef struct{
	DataType type;
	char *name;
}Column;


typedef struct{
	StatementType type;
	MetaType m_type;   //for meta statement only
	Row *row_to_insert; //for insert statement only
	char *table_name;  //for create statement only
	Column **columns;  // ***
	int number_of_columns;
}Statement;

typedef struct{
	Row *rows;
	Column **columns;
	uint32_t row_number;
	uint32_t number_of_columns;
	void *pages[MAX_PAGES];
	char *name;
	uint32_t ROW_SIZE;
	uint32_t ROWS_PER_PAGE;
}Table;

void create_table(char *name, Column **columns, int number_of_columns, Table *table)
{
	(table)->number_of_columns = number_of_columns;
	(table)->name = (char *)malloc(strlen(name));
	strcpy((table)->name, name);
	(table)->columns = (Column **)malloc(sizeof(Column *) * number_of_columns);
	int i;
	int size = 0;
	for (i = 0; i < number_of_columns; i++)
	{
		table->columns[i] = (Column *)malloc(sizeof(Column));
		table->columns[i]->type = columns[i]->type;
		if (columns[i]->type == INT)
		{
			size += INT_SIZE;
		}
		else
		{
			size += STRING_SIZE;
		}
		table->columns[i]->name = columns[i]->name;
	}
	table->ROW_SIZE = size;
	table->ROWS_PER_PAGE = PAGE_SIZE / size;
	table->row_number = 0;
	return;
}


//these should now exist inside the table struct
const uint32_t ID_SIZE = sizeof(uint32_t);
//row_size += sizeof(data_type) for every data type
//const uint32_t ROW_SIZE = ID_SIZE;
//const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t ID_OFFSET = 0;

void destroy_input_buffer(InputBuffer *buf)
{
	free(buf);
}

void prompt()
{
	write(STDOUT_FILENO, "db > ", 5);
}

int read_prompt(char* buf, int size)
{
	return read(STDIN_FILENO, buf, size);
}

void* row_slot(Table *table, uint32_t row_number)
{
	uint32_t page_number = row_number / table->ROWS_PER_PAGE;
	void *page = table->pages[page_number];
	if (page == NULL)
	{
		//allocate mem for page if not used before
		page = table->pages[page_number] = malloc(PAGE_SIZE);
	}
	//we want to return the exact offset of memory to allocate the new row
	//so that would look like page + byte_offset (row number * row size)
	uint32_t row_offset = row_number % table->ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * table->ROW_SIZE;
	return page + byte_offset;
}

void serialize(Row *source, void *destination, int number_of_columns)
{
	//memcpy(void dest[], void src[], int n); //copy n bytes from src to dest
	int i;
	int offset = 0;
	for (i = 0; i < number_of_columns; i++)
	{
		if (source->data[i]->type == INT)
		{
			memcpy(destination + offset, source->data[i]->data, INT_SIZE);
			offset += INT_SIZE;
		}
		else
		{
			memcpy(destination + offset, source->data[i]->data, STRING_SIZE);
			offset += STRING_SIZE;
		}
	}


}

void deserialize(Row *destination, void *source, Table *table)
{
	int i;
	int offset = 0;
	for (i = 0; i < table->number_of_columns; i++)
	{
		destination->data[i] = (Data *)malloc(sizeof(Data));
		//from here, we need to write the data, the type of data, and the size(which we get from the type)
		//I think the only way to do this is to pass the table so that we have the columns to reference
		//We should expect to be able to pull the data in the order of the columns
		if (table->columns[i]->type == INT)
		{
			//pull 4 bytes
			destination->data[i]->data = malloc(INT_SIZE);
			memcpy(destination->data[i]->data, source + offset, INT_SIZE);
			destination->data[i]->type = INT;
			destination->data[i]->size = INT_SIZE;
			offset += INT_SIZE;
		}
		else
		{
			//pull 256 bytes
			destination->data[i]->data = malloc(STRING_SIZE);
                        memcpy(destination->data[i]->data, source + offset, STRING_SIZE);
                        destination->data[i]->type = STRING;
                        destination->data[i]->size = STRING_SIZE;
                        offset += STRING_SIZE;
		}
	}
}

/*
    table name
    column titles and data types
    then until EOF, each line is a row
*/

void write_to_disk(Table *table){
    int fd = open("data.txt", O_RDWR);
    if (fd == -1){
        printf("error in write_to_disk\n");
    }
    write(fd, table->name, strlen(table->name));
}

void read_from_disk(){
    char buf[1000];
    int fd = open("data.txt", O_RDONLY);
    if (fd == -1)
    {
        printf("error in read_from_disk\n");
    }
    read(fd, buf, sizeof(buf));
    write(STDOUT_FILENO, buf, strlen(buf));
    write(STDOUT_FILENO, NEWLINE, 1);
}

void print_row(Row *row)
{
	int i = 0;
	char buf[256];

	while(row->data[i] != NULL)
	{
		printf("%s\t", (char *)row->data[i]->data);


		//strcpy(buf+1, (char *)row->data[i]->data);
		//write(STDOUT_FILENO, buf, strlen(buf));
		//memset(buf, 0, sizeof(buf));
		i++;
	}
}

void print_col(Column *col)
{
	char buf[256];
	buf[0] = ' ';
	buf[1] = '|';
	buf[2] = ' ';
	if (col->type == INT)
	{
		strcpy(buf+3, col->name);
		strcpy(buf+strlen(col->name)+3, " INT");
		write(STDOUT_FILENO, buf, strlen(buf));
	}
	else if (col->type == STRING)
	{
		strcpy(buf+3, col->name);
		strcpy(buf+strlen(col->name)+3, " STRING");
		write(STDOUT_FILENO, buf, strlen(buf));
	}
}

void print_create_usage_info()
{
	printf("usage info: create 'table name' 'column name' 'data type', ...\n" );
}

InputBuffer* new_input_buffer()
{
        InputBuffer *buf = malloc(sizeof(InputBuffer));
        memset(buf->buffer, 0, sizeof(buf->buffer));
        buf->buffer_len = 0;
        buf->buffer_size = 256;
        return buf;
}

MetaResult meta_command(Statement *statement, Table *table)
{
	//exit
	//if (strncmp(buffer, ".exit", len-1) == 0)
	if (statement->m_type == EXIT)
	{
	    write_to_disk(table);
		exit(0);
	}
	//tables (show current tables
	//if (strncmp(buffer, ".tables", len-1) == 0)
	if (statement->m_type == TABLE)
	{
		if (table == NULL)
		{
			printf("no tables created (tables == NULL)\n");
			return META_SUCCESS;
		}
		int i;
		int j = table->number_of_columns;
		printf("\ntable name: %s\n\n", table->name);
		for (i = 0; i < j; i++)
		{
			print_col(table->columns[i]);
			//deserialize(row, row_slot(table, i));
			//print_row(row);
		}
		printf("\n");

                printf("\t");
                for (i = 0; i < table->row_number; i++)
                {
                        Row *row = (Row *)malloc(sizeof(Row));
                        row->data = (Data **)malloc(sizeof(Data));
                        deserialize(row, row_slot(table, i), table);
                        print_row(row);
                }
		printf("\n\n");

		return META_SUCCESS;
	}
	else{
		return META_UNRECOGNIZED;
	}
}

PrepareResult prepare_statement(char *buffer, int len, Statement *statement, Table *table)
{
	if (strncmp(buffer, ".", 1) == 0)
	{
		//if (meta_command(buffer, len, table) != META_UNRECOGNIZED){return PREPARE_SUCCESS;}
		statement->type = META;
		if (strncmp(buffer, ".exit", len-1) == 0)
		{
			statement->m_type = EXIT;
			return PREPARE_SUCCESS;
		}
		else if (strncmp(buffer, ".tables", len-1) == 0)
		{
			statement->m_type = TABLE;
			return PREPARE_SUCCESS;
		}
		else
		{
			return PREPARE_UNRECOGNIZED;
		}
	}
	else if (strncmp(buffer, "create", 6) == 0)
	{
		statement->type = CREATE;
		//we have to get all entered columns metadata
		//char *strtok(char * str , char *delim)
		strtok(buffer, " "); // this should be CREATE
		char *table_name = strtok(NULL, " "); //this should be the name of the table we are creating
		statement->table_name = (char *)malloc(strlen(table_name));
		strcpy(statement->table_name, table_name);
		printf("statement->table_name: %s\n", statement->table_name);
		char *subtoken = strtok(NULL, ","); //each one of these subtokens should be a column name and a datatype
		int i = 0;
		char* column_name = (char *)malloc(100);
		char* data_type = (char *)malloc(100);
		while (subtoken != NULL) //I want to iterate over an arbitrary number of columns
		{
		    memset(column_name, 0, 100);
			token_helper(subtoken, column_name, data_type);
			printf("subtoken: %s\n", subtoken);
			printf("column name : %s\n", column_name);
			printf("data type : %s\n", data_type);
			statement->columns[i] = (Column *)malloc(sizeof(Column));
			statement->columns[i]->name = malloc(strlen(column_name));
			strcpy(statement->columns[i]->name, column_name);
			if (strncmp(data_type, "int", 3) == 0)
			{
				//statement->columns[i]->type = (DataType *)malloc(sizeof(DataType));
				statement->columns[i]->type = INT;
			}
			else if (strncmp(data_type, "string", 6) == 0)
			{
				//statement->columns[i]->type = (DataType *)malloc(sizeof(DataType));
				statement->columns[i]->type = STRING;
			}
			else
			{
				print_create_usage_info();
			}
			statement->number_of_columns++;
			i++;
			//free(column_name);
			//free(data_type);
			subtoken = strtok(NULL, ",");
		}
		return PREPARE_SUCCESS;
 	}
	else if (strncmp(buffer, "select", len-1) == 0)
	{
		statement->type = SELECT;
		return PREPARE_SUCCESS;
	}
	else if (strncmp(buffer, "insert", 6) == 0)
	{
		statement->type = INSERT;
                //i need to malloc space for row_to_insert and then malloc space for every piece of data to enter
                //i need to strtok my inputs into row_to_insert[0], row_to_insert[1], ..., row_to_insert[n]
                //as well as set data types, right now I may be only supporting one table
                //to be honest I'm tired and cannot think any longer

		//we should iterate column_num times
		//check what type we are dealing with
		//store with the appropriate scan code into the column_num index of statement->row_to_insert.whatever

		//we are creating a single row (but an array of data within that row)
		statement->row_to_insert = (Row *)malloc(sizeof(Row));
		statement->row_to_insert->data = (Data **)malloc(sizeof(Data));

		//TODO check if table exists
		strtok(buffer, " "); // first call will return 'insert'

		int i;
		for (i = 0; i < table->number_of_columns; i++)
		{
			statement->row_to_insert->data[i] = malloc(sizeof(Data));
			//Data: DataType type, void *data, uint32_t size
			void *data = strtok(NULL, " ");
			DataType type = table->columns[i]->type;
			if (type == INT)
			{
				data = (int *)data;
			}
			else
			{
				data = (char *)data;
			}
			statement->row_to_insert->data[i]->data = data;
			//printf("statement->row_to_insert->data[i]->data : %s", statement->row_to_insert->data[i]->data);
			statement->row_to_insert->data[i]->type = table->columns[i]->type;
		}

		//sscanf(char *buffer, "%s", where to store)
		//sscanf(buffer, "insert %d", &statement->row_to_insert->id);
		return PREPARE_SUCCESS;
	}
	return PREPARE_UNRECOGNIZED;
}



void token_helper(char *str, char* column_name, char* data_type)
{
    //get rid of leading spaces!
  	int i = 0;

    if (str[0] == ' '){
        while(str[i] == ' '){
            i++;
        }
    }
    int n = 0;
	while(str[i] != ' ')
	{
	 	column_name[n++] = str[i];
		i++;
	}
	i++;
	column_name[i] = '\0';
	int j = 0;
	while (str[i] != '\0')
	{
		data_type[j++] = str[i++];
	}
	j++;
	data_type[j] = '\0';


}

ExecuteResult execute_statement(Statement *statement, Table *table)
{
	if (statement->type == META)
	{
		meta_command(statement, table);
	}
	else if (statement->type == INSERT)
	{
		//again, iterate column_num times
		//serializing each piece of data from statement->row_to_insert
		//serialize will check the data type
		serialize(statement->row_to_insert, row_slot(table, table->row_number), statement->number_of_columns);
		table->row_number++;
	}
	else if (statement->type == CREATE)
	{
		//printf("statement table name: %s\n", statement->table_name);
		create_table(statement->table_name, statement->columns, statement->number_of_columns, table);
	}
	else if (statement->type == SELECT)
	{
	    printf("\n\n\t%s\n\t________\n\n", table->name);
		int i;
		printf("\t");
		for (i = 0; i < table->row_number; i++)
		{
			Row *row = (Row *)malloc(sizeof(Row));
			memset(row, 0, sizeof(Row));
			row->data = (Data **)malloc(sizeof(Data));
			memset(row->data, 0, sizeof(Data));
			deserialize(row, row_slot(table, i), table);
			print_row(row);
		}
		printf("\n\n");
	}
	//printf("ID: %d\n", statement->row_to_insert.id);
	return EXECUTE_SUCCESS;
}

int main()
{
    read_from_disk();
	Table *table = malloc(sizeof(Table));
	Statement *statement = malloc(sizeof(Statement));
	statement->columns = (Column **)malloc(sizeof(Column *) * 10); //support 10 columns

	//Table *table = (Table *)malloc(sizeof(Table));
	//initiate REPL
	for (;;)
	{
		InputBuffer *buf = new_input_buffer();


		prompt();
		//READ
		buf->buffer_len = read_prompt(buf->buffer, buf->buffer_size);
		//PREPARE
		switch (prepare_statement(buf->buffer, buf->buffer_len, statement, table))
		{
			case(PREPARE_SUCCESS):
			{
				execute_statement(statement, table);
				break;
			}
			case(PREPARE_UNRECOGNIZED):
			{
				printf("statement unrecognized\n");
				break;
			}
		}
		destroy_input_buffer(buf);
	}
	return 0;
}
