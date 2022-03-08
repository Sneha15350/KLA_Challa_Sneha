import yaml
from yaml.loader import SafeLoader
import datetime
import time
import threading
s = 0

with open('Milestone2A.yaml', 'r') as f:
    data = yaml.load(f, Loader=SafeLoader)
#log.write(type(data))
import csv 
ls=[]
with open('Milestone2A_DataInput1.csv') as csv_file: 
    csv_reader = csv.reader(csv_file, delimiter=',') 
    next(csv_reader)
    for row in csv_reader: 
        ls.append(row)

#log.write(ct)
log = open('Milestone2A_log.txt','w')
ct = None
def printingTasks(mainflow,subflow,task,function_name,input1,input2):
    global ct
    log.write(str(datetime.datetime.now())+';'+mainflow+'.'+subflow+'.'+task+' Entry'+'\n')
    log.write(str(datetime.datetime.now())+';'+mainflow+'.'+subflow+'.'+task+' Executing '+function_name+' ('+input1+','+input2+')'+'\n')
    time.sleep(int(input2))
    log.write(str(datetime.datetime.now())+';'+mainflow+'.'+subflow+'.'+task+' Exit'+'\n')
    ct = datetime.datetime.now()
def printingTasksDataLoad(mainflow,subflow,task,function_name,input1):
    global ct
    log.write(str(datetime.datetime.now())+';'+mainflow+'.'+subflow+'.'+task+' Entry'+'\n')
    log.write(str(datetime.datetime.now())+';'+mainflow+'.'+subflow+'.'+task+' Executing '+function_name+' ('+input1+')'+'\n')
    log.write(str(datetime.datetime.now())+';'+mainflow+'.'+subflow+'.'+task+' Exit'+'\n')

def printingFlows(dct1,mainwflow,tasks,tasks2):
    global s
    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+' Entry'+'\n')
    if dct1['Type']=='Flow' and dct1['Execution']=='Sequential':


        for tasks1 in dct1['Activities'].keys():


            if(dct1['Activities'][tasks1]['Type']=='Task'):
                if(dct1['Activities'][tasks1]['Function'] == 'TimeFunction'):
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+'.'+tasks1+' Entry'+'\n')
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+'.'+tasks1+' Executing '+dct1['Activities'][tasks1]['Function']+' ('+dct1['Activities'][tasks1]['Inputs']['FunctionInput']+','+dct1['Activities'][tasks1]['Inputs']['ExecutionTime']+')'+'\n')
                    time.sleep(int(dct1['Activities'][tasks1]['Inputs']['ExecutionTime']))
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+'.'+tasks1+' Exit'+'\n')
                    s = s + int(dct1['Activities'][tasks1]['Inputs']['ExecutionTime'])
                elif(dct1['Activities'][tasks1]['Function'] == 'DataLoad'):
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+'.'+tasks1+' Entry'+'\n')
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+'.'+tasks1+' Executing '+dct1['Activities'][tasks1]['Function']+' ('+dct1['Activities'][tasks1]['Inputs']['Filename']+')'+'\n')
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+'.'+tasks1+' Exit'+'\n')
    if dct1['Type']=='Flow' and dct1['Execution']=='Concurrent':
        for tasks1 in dct1['Activities'].keys():
            if(dct1['Activities'][tasks1]['Type']=='Task'):
                if(dct1['Activities'][tasks1]['Function'] == 'TimeFunction'):
                    t5 = threading.Thread(target=printingTasks, args=(mainwflow,tasks,tasks2,dct1['Activities'][tasks1]['Function'],dct1['Activities'][tasks1]['Inputs']['FunctionInput'],dct1['Activities'][tasks1]['Inputs']['ExecutionTime'],))
                    t5.start()
                    s = s + int(dct1['Activities'][tasks1]['Inputs']['ExecutionTime'])
                elif(dct1['Activities'][tasks1]['Function'] == 'DataLoad'):
                    t6 = threading.Thread(target=printingTasksDataLoad, args=(mainwflow,tasks,tasks2,dct1['Activities'][tasks1]['Function'],dct1['Activities'][tasks1]['Inputs']['Filename'],))
                    t6.start()





                    
    time.sleep(s)
    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks2+' Exit'+'\n')




for mainwflow in data.keys():
    log.write(str(datetime.datetime.now())+';'+mainwflow+' Entry'+'\n')
    dct=data[mainwflow]
    if dct['Type']=='Flow' and dct['Execution']=='Sequential':
        for tasks in dct['Activities'].keys():
            if(dct['Activities'][tasks]['Type']=='Task'):
                print(s)
                time.sleep(s)
                if(dct['Activities'][tasks]['Function'] == 'TimeFunction'):
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Entry'+'\n')
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Executing '+dct['Activities'][tasks]['Function']+' ('+dct['Activities'][tasks]['Inputs']['FunctionInput']+','+dct['Activities'][tasks]['Inputs']['ExecutionTime']+')'+'\n')
                    time.sleep(int(dct['Activities'][tasks]['Inputs']['ExecutionTime']))
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Exit'+'\n')
                elif(dct['Activities'][tasks]['Function'] == 'DataLoad'):
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Entry'+'\n')
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Executing '+dct['Activities'][tasks]['Function']+' ('+dct['Activities'][tasks]['Inputs']['Filename']+')'+'\n')
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Exit'+'\n')






            if(dct['Activities'][tasks]['Type']=='Flow'):
                
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Entry'+'\n')
                    dct1 = dct['Activities'][tasks]
                

                    if dct1['Type']=='Flow' and dct1['Execution']=='Sequential':
                        for tasks1 in dct1['Activities'].keys():

                            if(dct1['Activities'][tasks1]['Type']=='Task'):
                                if(dct1['Activities'][tasks1]['Function'] == 'TimeFunction'):
                                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks1+' Entry'+'\n')
                                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks1+' Executing '+dct1['Activities'][tasks1]['Function']+' ('+dct1['Activities'][tasks1]['Inputs']['FunctionInput']+','+dct1['Activities'][tasks1]['Inputs']['ExecutionTime']+')'+'\n')
                                    time.sleep(int(dct1['Activities'][tasks1]['Inputs']['ExecutionTime']))
                                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks1+' Exit'+'\n')
                                elif(dct1['Activities'][tasks1]['Function'] == 'DataLoad'):
                                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks1+' Entry'+'\n')
                                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks1+' Executing '+dct['Activities'][tasks]['Function']+' ('+dct['Activities'][tasks]['Inputs']['Filename']+')'+'\n')
                                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+'.'+tasks1+' Exit'+'\n')
    
                        
                    if dct1['Type']=='Flow' and dct1['Execution']=='Concurrent':
                        for tasks2 in dct1['Activities'].keys():
                            if(dct1['Activities'][tasks2]['Type']=='Task'):
                                if(dct1['Activities'][tasks2]['Function'] == 'TimeFunction'):
                                    t1 = threading.Thread(target=printingTasks, args=(mainwflow,tasks,tasks2,dct1['Activities'][tasks2]['Function'],dct1['Activities'][tasks2]['Inputs']['FunctionInput'],dct1['Activities'][tasks2]['Inputs']['ExecutionTime'],))
                                    t1.start()
                                    s = s + int(dct1['Activities'][tasks2]['Inputs']['ExecutionTime'])
                                elif(dct1['Activities'][tasks2]['Function'] == 'DataLoad'):
                                    t3 = threading.Thread(target=printingTasksDataLoad, args=(mainwflow,tasks,tasks2,dct1['Activities'][tasks2]['Function'],dct1['Activities'][tasks2]['Inputs']['Filename'],))
                                    t3.start() 

                            elif(dct1['Activities'][tasks2]['Type']=='Flow'):
                                    t2 = threading.Thread(target = printingFlows, args=(dct1['Activities'][tasks2],mainwflow,tasks,tasks2))
                                    t2.start()
                    time.sleep(s)
                    time.sleep(s)
                    log.write(str(datetime.datetime.now())+';'+mainwflow+'.'+tasks+' Exit'+'\n')

                        
                    


         
                                   
         
        
               
        log.write(str(datetime.datetime.now())+';'+mainwflow+' Exit'+'\n')



                                            






            
            






                                            






            
            


