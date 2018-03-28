#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <ctype.h>
#include <semaphore.h>
#include <sys/mman.h>


int size;
int nusers;
char filename[20];
int flag = 0;

struct tuple{
	char userid[5];
	char topic[16];
	int score;
};

struct buffer{
	struct tuple* bufferptr;
	sem_t sem;
	sem_t fullmap;
	sem_t emptymap;
	
	//pthread_mutex_t lock;
	//pthread_cond_t full;
	//pthread_cond_t empty;	
	
	int in;
	int out;
	int nitems;
	 
	int nprod;
	int ncons;
	int done;
	int qid;		
};

struct Node{
	char userid[5];
	char topic[16];
	int total;
	struct Node *next;
};
void* mapper(void *);
void* reducer(void *);

//================================= MAPPER ==================================//


void *mapper(void *buff){
	FILE *fp1;	
	char sstring[25];
	char action;
	char* unit;
	int flag = 0;
	int ptr = 0, finduser=0;
	
	bool set;
	
	struct tuple tuple1;
	struct buffer* array = (struct buffer *)buff;
	struct buffer* temp;
	int pt_id[nusers];
	
	sleep(1);


	fp1 = fopen(filename, "r");			
	
	for(int i=0; i<nusers; ++i){	
		pt_id[i] = -1;		//initialize userid array by -1 
	};
	
	
	if(fp1==NULL){
		printf("error: unable to open file");
		return NULL;
	}	
	
	while(1){
		flag = 0;
		unit = fgets(sstring, 26, fp1);
		temp = array;

		//fetch and build
			//check if right format and form tuple1
		if(unit != NULL){											
			if(sstring[0] == '(' && sstring[23] == ')') {				
				if(sstring[24]==',' || sstring[24] == '\0') {
					if(sstring[5]==',' && sstring[7] == ',') {
						
						for(int j=0;j<4;j++){					//copy userid to tuple struct
							tuple1.userid[j] = sstring[1+j];
						}
						tuple1.userid[4]='\0';
						//printf("(%s, %s, %d)", tuple1.userid, tuple1.topic, tuple1.score);
						
						for(int j=0; j<nusers; ++j){		//pt_id is array of userids
							if(pt_id[j] == atoi(tuple1.userid)){	//if id matches->existing userid-> set flag = 1 increment ptr
										
								flag = 1;
								if(ptr==0){
									ptr++;
								}
								break;
							}
						}
						
						if(flag==0){				//flag = 0 -> new id -> store in array
							pt_id[ptr] = atoi(tuple1.userid);
							if(ptr < nusers){
								ptr++;
							}
						}
						//calculate weight by action
						action = sstring[6];
						switch(action){
							case 'P':
								tuple1.score = 50;
								break;																																																																																																																																																																																																																																																																																																																																																																														
							case 'S':
								tuple1.score = 40;
								break;
							case 'C':
								tuple1.score = 30;
								break;
							case 'L':
								tuple1.score = 20;
								break;
							case 'D':
								tuple1.score = -10;
								break;
							default: pthread_exit((void*) buff);
						}
						
						
						//store topic in tuple
					
						for(int i=0; i<15; i++){
							tuple1.topic[i] = sstring[8+i];
						}
						tuple1.topic[15] = '\0';
						
						//finduser = usertag(sr no.) ex. 0000 = 0, 0111 = 1 etc.
				
						for(int k=0; k<nusers; ++k){
							if(pt_id[k] == atoi(tuple1.userid)){
								finduser = k;
								break;
							}
						}
						//temp buffer 	
						temp = temp + finduser;
						//***********************************************************************************************//				
						
						//pthread_mutex_lock(&(temp->lock));  //START 
						sem_wait(&(temp->sem));//#
						
						struct tuple *element = temp->bufferptr;//element is a tuple in temp buffer
						while(temp->nitems == size){     //// check if buffer is full (FULL)
							//pthread_cond_wait(&(temp->full), &(temp->lock)); //block if buffer is full
							sem_post(&(temp->sem));//#
							sem_wait(&(temp->fullmap));//#
							sem_wait(&(temp->sem));		//#
						}
						
						//copy above constructed tuple1 into element which is a tuple of the temp buffer  
						element[temp->in].score = tuple1.score;				//insert
						strcpy(element[temp->in].userid, tuple1.userid);	//insert
						strcpy(element[temp->in].topic, tuple1.topic);		//insert
						(temp->in) = ((temp->in)+1) % size;					//
						(temp->nitems)++;							
						(temp->nprod)++;
						//printf("qid = %d, nprod =  %d, nitems = %d \n", temp->qid, temp->nprod, temp->nitems);
						//pthread_cond_signal(&(temp->empty));  					
						//pthread_mutex_unlock(&(temp->lock));   //END
						
						//////
						//printf("(%s,%s,%d)\n", element[temp->in].userid, element[temp->in].topic, element[temp->in].score);
						
						
						//////
						
						
						
						sem_post(&(temp->sem)); //#	
						sem_post(&(temp->emptymap));	//#
					}
				}
				else{
					//printf("format exception");
				}
			}
			else{
				//printf("format exception");
			}
		}
		
		
		
		
		
		else if(unit==NULL){		//end of file
			for(int i=0; i < nusers; i++){
				//pthread_mutex_lock(&((array+i)->lock));				
				sem_wait(&((array+i)->sem));		//#
				(array+i)->done = 1;							
				//pthread_cond_broadcast(&((array+i)->empty));
				//pthread_cond_broadcast(&((array+i)->full));
				for(int k=0; k < nusers; k++){	//#
						
					sem_post(&((array+k)->emptymap));	//#
					sem_post(&((array+k)->fullmap));	//#
				}
				//pthread_mutex_unlock(&((array+i)->lock));
				sem_post(&((array+i)->sem));	//#				
			}

		}

		for(int j=0; j<nusers; j++){
			//pthread_mutex_lock(&((array+j)->lock));			
			
			sem_wait(&((array+j)->sem));		//#
			
			if(j==0){
				set = array->done;
			}
			else{
				set = set && (array+j)->done;
			}
			//pthread_mutex_unlock(&((array+j)->lock));
			sem_post(&((array+j)->sem));		//#
			//sem_post(&((array+j)->emptymap));		
		}

		if(set){
			//printf("producer is exiting\n");
			break;
		}
	}
	
	for(int j = 0; j<nusers; ++j){
		//printf("%d", nusers);
		//printf("%d\t ---------------", pt_id[j]);
	}
	//printf("\n");
	pthread_exit((void*)buff);
}

//================================= REDUCER ==================================//

void *reducer(void *redc){
	int nodec = 0;
	struct buffer *redbuff = (struct buffer *)redc;
	//struct Node* hdr = malloc(sizeof(struct Node));
	struct Node* hdr = mmap(NULL, sizeof(struct Node), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	
	//sleep(1);
	struct tuple *element = redbuff->bufferptr;
	
	/*sem_init(&(redbuff->sem),0,1);
	sem_init(&(redbuff->fullmap),0,0);
	sem_init(&(redbuff->emptymap),0,1);
	*/
	
	sleep(1);
	while(1){
		//pthread_mutex_lock(&(redbuff->lock));		//START
		
		sem_wait(&(redbuff->sem));	//#
		int flag=0;									//initialize flag
	
		while((redbuff->nitems==0) && !(redbuff->done)){	//empty and not done 
			//pthread_cond_wait(&(redbuff->empty), &(redbuff->lock));
			sem_post(&(redbuff->sem));
			sem_wait(&(redbuff->emptymap));
			sem_wait(&(redbuff->sem));	
		}
		
		if((redbuff->nitems==0) && (redbuff->done)){
			//pthread_mutex_unlock(&(redbuff->lock));			//end if empty and done 
			sem_post(&(redbuff->sem));
			break;
		}
		
		//struct Node* Node1 = (struct Node*)malloc(sizeof(struct Node));	//update Node1 
		struct Node* Node1 = (struct Node*)mmap(NULL, sizeof(struct Node), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
		Node1->total = element[redbuff->out].score;
		strcpy(Node1->userid, element[redbuff->out].userid);
		strcpy(Node1->topic, element[redbuff->out].topic);
		
		Node1->next = NULL;
		
		if(nodec == 0){
			hdr = Node1;		//head
			nodec++;
		}
		else if(nodec>0){
			struct Node* curr;	//initialize curr node to traverse the LL
			curr=hdr;
			
			while(1){
				if(!strcmp(curr->topic, Node1->topic)){  //same topic -> !0 => set flag
					curr->total += Node1->total;
					flag = 1;
					break;
				}
				if(curr->next == NULL){
					break;
				}
				else{
					curr = curr->next;
				}
			}
			
			if(flag==0){				//topic change node change
				curr->next = Node1;
				nodec++;
			}
		}
	//###################################################//
		
		(redbuff->out) = ((redbuff->out)+1)%size;
		(redbuff->nitems)--;
		(redbuff->ncons)++;
		//pthread_cond_signal(&(redbuff->full));		//if done && empty signal for full
		sem_post(&(redbuff->fullmap));
		
		//pthread_mutex_unlock(&(redbuff->lock));		//END 
		sem_post(&(redbuff->sem));
		//sem_post(&(redbuff->emptymap));
		//sem_post(&(redbuff->fullmap));
		
		
	}
	
	struct Node* tempnode;
	tempnode= hdr;
	
	while(1){
		if(tempnode->next != NULL){		//
			printf("(%s, %s, %d) \n", tempnode->userid, tempnode->topic, tempnode->total);
			tempnode = tempnode->next;
		}
		else if(tempnode->next == NULL){
			printf("(%s, %s, %d) \n", tempnode->userid, tempnode->topic, tempnode->total);
			break;
		}
	}
	pthread_exit((void*)redc);
}
	
		
//================================= COMBINER ==================================//		
		

int main(int argc, char* argv[]){
	void *stat;
	pthread_t mapperthread;
	
	size = atoi(argv[1]);
	nusers = atoi(argv[2]);
	strcpy(filename, argv[3]);
	pthread_t reducerthread[nusers];
	
	//struct buffer *buff = malloc(nusers * sizeof(struct buffer));
	struct buffer *buff = mmap(NULL, nusers * sizeof(struct buffer), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	
	//struct buffer *buff; 
	//printf("(%s)", buff[1].bufferptr->userid );
	for(int i=0; i<nusers; i++){
		//buff[i].bufferptr = malloc(size * sizeof(*buff[i].bufferptr));
		buff[i].bufferptr = mmap(NULL, size * sizeof(*buff[i].bufferptr), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
		buff[i].in=0;
		buff[i].out=0;
		buff[i].nitems=0;
		buff[i].nprod=0;
		buff[i].ncons=0;
		buff[i].done=0;
		sem_init(&(buff[i].sem),1,1);
		sem_init(&(buff[i].fullmap), 1, 0);
		sem_init(&(buff[i].emptymap),1,0);
	}
	
	/*
	///// TEST //////
	mapper((void *)buff);
	for(int j=0; j<nusers; j++){
			if(fork()==0){
				//printf("here....");
				buff[j].qid = j;
				reducer(&buff[j]);
			}
	}
	///// TEST /////
	//*/
	int pid;
	//pid = fork();
	
	
	
	for(int j =0; j<nusers; j++){
		if(fork()==0){
			buff[j].qid = j;
			reducer(&buff[j]);
		}
	}
	mapper((void*)buff);
	
	/*
	/////
	switch(pid){
		
		default:
			
			printf("child - mapper \n");		
			mapper((void *)buff);
			exit(EXIT_SUCCESS);
		
		case 0:
			printf("parent - Reducer \n");
			for(int j=0; j<nusers; j++){
				//printf("here....");
				
				buff[j].qid = j;
				reducer(&buff[j]);
				//pthread_create(&reducerthread[j], NULL, reducer, &buff[j]); //create reducer thread
			}
		
			
			
			//exit(EXIT_SUCCESS);
		
			
	}
	//////////////////
	*/
	
		//for(int i=0; i<nusers; i++){
			//printf("(%s)", buff[i].bufferptr->userid );
		//}
		/*
	pthread_create(&mapperthread, NULL, mapper, (void*)buff); //create produer thread
	
	for(int j=0; j<nusers; j++){
		//printf("here....");
		buff[j].qid = j;
		pthread_create(&reducerthread[j], NULL, reducer, &buff[j]); //create reducer thread
	}
	pthread_join(mapperthread, &stat); 
	
	
	for(int j=0;j < nusers; j++){
		pthread_join(reducerthread[j], &stat);
	}
		
	free(buff);
	*/
	
	return 0;
}

