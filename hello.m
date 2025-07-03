#include <objc/objc.h>
#import <stdio.h>

@interface HelloWorld : Object
{
}
-(void) hello;
@end

@implementation HelloWorld
-(void) hello
{
    printf("Hello World");
}
@end

int main(int argv, char *argc[])
{
    id o = [HelloWorld new];
    [o hello];
}

