



typedef struct{

        char buffer[256];
        int buffer_len;
        int buffer_size;

}InputBuffer;


InputBuffer* new_input_buffer(void);
void prompt(void);
int read_prompt(char*, int);
