#! /usr/bin/env python3

# ESRU 2017

# This is the back end simulation service for the calibro platform. Once
# invoked, this program will run in an infinite loop until a fatal error
# occurs or the process is terminated. Simulation jobs, initiated by the
# BPA front end, will be spawned as seperate processes in parallel.
# Communication between the front and back ends is accomplished by a
# shared directory, see documentation for the file structure of this
# directory. This can be shared via an online platform such as Dropbox
# to enable online communication between front and back ends on
# different computers.

# Command line options:
# -h, --help  - displays help text
# -d, --debug - service prints debug information to standard out,
#               and jobs print debug information to "[jobID].log"
#               in the job folder (../jobs/job_[jobID]).

# Command line arguments:
# 1: path to shared folder
# 2: [optional] dispatch interval in seconds

import sys
from os.path import isfile,isdir,realpath,dirname,basename
from os import devnull,makedirs,chdir,kill,remove
from subprocess import run,Popen,PIPE
from time import sleep,time
from multiprocessing import Process,Pipe
from datetime import datetime
from shutil import copytree,copyfile,rmtree
from glob import glob
import signal
from mysql import connector
import ctypes
from setproctitle import setproctitle

### FUNCTION: runJob
# This runs a performance assessment on an ESP-r model. This should be run in a
# seperate process, otherwise errors will terminate the main script. If
# debugging is active, a log file will be written out to "[jobID].log". If the
# job fails due to an error, a file called "[jobID.err]" will be written out
# containing the error message.

def runJob(s_jobID,s_name,s_archive,b_debug,con,s_shareDir):

    setproctitle('caliserv'+s_jobID)

    s_jobDir=getJobDir(s_jobID)
    if isdir(s_jobDir):
        rmtree(s_jobDir)
    makedirs(s_jobDir)
    chdir(s_jobDir)
    makedirs('inputs')

    # Create a directory for the outputs.
    makedirs('outputs')

    if b_debug:
        f_log=open(s_jobID+'.log','w')
        curDateTime=datetime.now()
        s_dateTime=curDateTime.strftime('%a %b %d %X %Y')
        f_log.write('*** JOB STARTED @ '+s_dateTime+' ***\nJobID: '+s_jobID+'\n')
    else:
        f_log=None

    ### FUNCTION: SIGTERMhandler
    # Terminate signal handler, calls jobError to ensure error output
    # is generated and uploaded.
    def SIGTERMhandler(signal,frame):
        jobError(s_jobID,'job recieved a terminate signal',15,b_debug,f_log,s_shareDir)

    signal.signal(signal.SIGTERM,SIGTERMhandler)

    # Send started signal.
    con.send(1)

    # Inputs archive path and extension.
    s_archiveServer=s_shareDir+'/Models/'+s_archive
    s_archiveInputs='./inputs/'+s_archive
    s_archiveWOext,s_ext=s_archive.split('.',1)

    # Get model.
    if b_debug: f_log.write('Retrieving inputs archive '+s_archiveServer+'\n')

    # # If there is an MD5 checksum passed from the front end, get checksum of the model file and compare them.
    # # Wait up to 100 seconds for model to appear and checksums to match.  
    # if not s_MD5sum is None:
    #     if b_debug: f_log.write('Checking MD5 checksum ...\n')  
    #     if b_debug: f_log.write('Checksum from front end: '+s_MD5sum+'\n')
    #     s_MD5sum=s_MD5sum.strip()
    #     i_count=0
    #     s_modelMD5=''
    #     while True:
    #         try:
    #             s_modelMD5=run(['md5sum',s_modelFile],check=True,sdtout=PIPE,text=True).stdout.split()[0]
    #         except:
    #             pass            
    #         if b_debug:f_log.write('Local checksum: '+s_modelMD5+'\n')
    #         if s_modelMD5==s_MD5sum: break
    #         i_count+=1
    #         if i_count>10:            
    #             # Timeout.
    #             jobError(s_jobID,'Timeout while waiting for model file "'+s_model+'"',11,b_debug,f_log,s_shareDir)
    #         if b_debug: f_log.write('Checksum does not match, waiting ...\n')
    #         sleep(10)
    #     if b_debug: f_log.write('Checksum verified.\n')
    # else:
    #     if b_debug: f_log.write('MD5 checksum not found.\n')

    # Now get the model.
    try:
        copyfile(s_archiveServer,s_archiveInputs)
    except:
        # Error - cannot find model.
        jobError(s_jobID,'error retrieving inputs archive "'+s_archive+'"',11,b_debug,f_log,s_shareDir)
    else:
        if s_ext=='zip':
            ls_extract=['unzip','-d','inputs']
        elif s_ext=='tar' or s_ext=='tar.gz':
            ls_extract=['tar','-xC','inputs','-f']
        else:
            jobError(s_jobID,'unrecognised inputs archive format (.zip, .tar, and .tar.gz supported)',16,b_debug,f_log,s_shareDir)
        try:
            run(ls_extract+[s_archiveInputs],check=True)
            remove(s_archiveInputs)
        except:
            jobError(s_jobID,'failed to extract inputs',16,b_debug,f_log,s_shareDir)

    # Check for input files, detecting multiple datasets.
    if not isfile('./inputs/bc.csv'):
        jobError(s_jobID,'boundary condition file not found',11,b_debug,f_log,s_shareDir)
    if not isfile('./inputs/calibro_input.csv'):
        jobError(s_jobID,'calibro input file not found',11,b_debug,f_log,s_shareDir)
    ls_datasets=[]
    if not isfile('./inputs/obs.csv'):
        ls_obsFile=glob('./inputs/*_obs.csv')
        if len(ls_obsFile)==0:
            jobError(s_jobID,'no observed data file(s) found',11,b_debug,f_log,s_shareDir)
        else:
            for s_obsFile in ls_obsFile:
                if not isfile(s_obsFile[:-8]+'_sim.csv'):                    
                    jobError(s_jobID,'no matching simulated data file found for observed data file '+s_obsFile[9:],11,b_debug,f_log,s_shareDir)
                else:
                    ls_datasets.append(s_obsFile[9:-8])
    else:
        if not isfile('./inputs/sim.csv'):
            jobError(s_jobID,'no simulated data file(s) found',11,b_debug,f_log,s_shareDir)

    # Assemble argument list.
    s_nameNoSpace=s_name.replace(' ','-')
    s_nameNoSpace=s_nameNoSpace.replace('_','-')
    ls_args=['-c',s_nameNoSpace,'-f','json,pdf','-b','inputs/bc.csv','-i','inputs/calibro_input.csv']
    if len(ls_datasets)==0:
        ls_args=ls_args+['-o','inputs/obs.csv','-s','inputs/sim.csv']
    else:
        ls_args=ls_args+['-o',','.join(['inputs/'+a+'_obs.csv' for a in ls_datasets]),
            '-s',','.join(['inputs/'+a+'_sim.csv' for a in ls_datasets])]
    ls_args=ls_args+['-r',','.join(['cal','sa','ret','train','ds'])]

    # Set handler so that the PAM will be killed if the job is killed.
    libc = ctypes.CDLL("libc.so.6")
    def set_pdeathsig(sig = signal.SIGKILL):
        def callable():
            return libc.prctl(1, sig)
        return callable

    if b_debug: 
        f_log.write('Calibration invoked with command: '+' '.join(['../../scripts/calibrino']+ls_args)+'\n')
#        f_log.write('Calibration invoked with command: '+' '.join(['calibrino']+ls_args)+'\n')

    # Send running signal.
    con.send(2)

    # Run calibration.
    proc=Popen(['../../scripts/calibrino']+ls_args,stdout=PIPE,stderr=PIPE,preexec_fn=set_pdeathsig(signal.SIGKILL))
#    proc=Popen(['calibrino']+ls_args,stdout=PIPE,stderr=PIPE,preexec_fn=set_pdeathsig(signal.SIGKILL))
    t_tmp=proc.communicate()
    if b_debug:
        f_log.write('\nCalibration finished, output follows:\n'+str(t_tmp[0].decode())+'\n')
    if proc.returncode!=0:
        jobError(s_jobID,'Calibration failed, error output follows:\n'+str(t_tmp[1].decode())+'\n',proc.returncode,b_debug,f_log,s_shareDir)

    # Upload job results.
    # Send uploading signal.
    con.send(3)

    # Move job outputs to outputs folder.
    run(['mv','-t','outputs','calibro_report.json','calibro_report.pdf'])

    if b_debug: f_log.write('Copying outputs to shared folder ...\n')
    s_DBjobDir=s_shareDir+'/Results/'+s_jobID
    try:
        rmtree(s_DBjobDir)
    except OSError:
        pass
    makedirs(s_DBjobDir)
    ls_outputs=glob('outputs/*')
    for s_output in ls_outputs:
        if b_debug: f_log.write('Copying '+basename(s_output)+' ...\n')
        if isfile(s_output):
            copyfile(s_output,s_DBjobDir+'/'+basename(s_output))
        elif isdir(s_output):            
            copytree(s_output,s_DBjobDir+'/'+basename(s_output))
        else:            
            jobError(s_jobID,'Could not copy file "'+s_output+'"\n',18,b_debug,f_log,s_shareDir)

    if b_debug: f_log.write('Done.\n')

    curDateTime=datetime.now()
    s_dateTime=curDateTime.strftime('%a %b %d %X %Y')
    if b_debug: f_log.write('\n*** JOB FINISHED @ '+s_dateTime+' ***\n')
    if b_debug: f_log.close()

    # Upload log file to outputs folder if present.
    if b_debug:
        copyfile(s_jobID+'.log',s_DBjobDir+'/log.txt')

    # Send exit signal then performance flag.
    con.send(4)
    b_dummy=True
    if b_dummy:
        # Just send a "compliant" signal.
        con.send(0)
    else:
        con.send(i_pFlag)

    # If job has got to this point, it was (hopefully) successful, so remove local job directory.
#    chdir('..')
#    rmtree(s_jobDir)

### END FUNCTION


### FUNCTION: jobError
# Writes an error file for a simulation job and exits with a fail code. If
# debugging is active, also writes the message to the log file. Adds a datetime
# stamp to all messages.
# Writes error to json and pdf as well.

def jobError(s_jobID,s_message,i_errorCode,b_debug,f_log,s_shareDir):
    curDateTime=datetime.now()
    s_dateTime=curDateTime.strftime('%a %b %d %X %Y')
    f=open(s_jobID+'.err','w')
    f.write(s_message+' @ '+s_dateTime)
    f.close()
    if b_debug: f_log.write('Error: '+s_message+' @ '+s_dateTime)

# Write error message into json and pdf.
    f_json=open('outputs/data.json','w')
    f_json.write('{"error": {\n'
                 '  "datetime": "'+s_dateTime+'",\n'
                 '  "code": "'+str(i_errorCode)+'",\n'
                 '  "message": "'+s_message.replace('"','')+'"\n'
                 '}}\n')
    f_json.close()
    f_pdf=open('outputs/report.tex','w')
    f_pdf.write('\\nonstopmode\n\documentclass{report}\n\\begin{document}\n'+
                'The job did not successfully complete.\n'+
                'An error occured at '+s_dateTime+'.\n'+
                'Error message was:\n\n'+
                '\\begin{verbatim}\n'+
                s_message+'\n'+
                '\end{verbatim}\n\n'+
                '\end{document}')
    f_pdf.close()
#    run(['sed','-e',r's/\_/\\\_/g','-i',s_pdf])
    f_pdfLog=open('pdflatex.out','w')
    run(['pdflatex','-output-directory=outputs',s_pdf],stdout=f_pdfLog)
    run(['pdflatex','-output-directory=outputs',s_pdf],stdout=f_pdfLog)
    f_pdfLog.close()

    # Copy results to shared folder.
    if b_debug: f_log.write('Copying outputs to shared folder ...\n')
    s_DBjobDir=s_shareDir+'/Results/'+s_jobID
    try:
        rmtree(s_DBjobDir)
    except OSError:
        pass
    makedirs(s_DBjobDir)
    copyfile('outputs/data.json',s_DBjobDir+'/data.json')
    copyfile('outputs/report.pdf',s_DBjobDir+'/report.pdf')
    if b_debug: f_log.write('Done.\n')    

    if b_debug: f_log.close()
    sys.exit(1)

### END FUNCTION


### FUNCTION: sleepTilNext
# Checks the time elapsed since startTime (obtained from time() built-in),
# compares it with r_interval, and sleeps for any remaining time.
def sleepTilNext(start_time,r_interval,b_debug):
    end_time=time()
    time_taken=end_time-start_time
    if b_debug: print("main.py: dispatch took "+'{:.2f}'.format(time_taken)+" seconds")
    if time_taken<r_interval:
        if b_debug: print('main.py: sleeping for '+'{:.2f}'.format(r_interval-time_taken)+' seconds')
        sleep(r_interval-time_taken)
    else:
        if b_debug: print("main.py: I'm late! I'm late!")

### END FUNCTION


### FUNCTION: getJobDir
# Takes the jobID and generates a job directory name from it.
# Creates a relative path from the location of this script,
# i.e. assumes that "../jobs" exists from the location of this script.
def getJobDir(s_jobID):
    s_jobDir=dirname(realpath(__file__))+'/../jobs/job_'+s_jobID
    return s_jobDir

### END FUNCTION


### FUNCTION: mainError
# Prints an error message from the main process to the screen and to the error log.
def mainError(s_msg,s_errlog):    
    curDateTime=datetime.now()
    s_dateTime=curDateTime.strftime('%a %d %b %X %Y')
    print('main.py warning @ '+s_dateTime+': '+s_msg)
    f_errlog=open(s_errlog+'_cur.txt','a')
    f_errlog.write(s_dateTime+': '+s_msg+'\n')
    f_errlog.close()

### END FUNCTION



        


def main():

    setproctitle('caliserv')

    # Set defaults.
    r_interval=15
    b_debug=False
    i_failLimit=10

    # Parse command line.
    i_argCount=0
    for arg in sys.argv[1:]:
        if arg[0]=='-':
            # This is an option.
            if arg=='-h' or arg=='--help':
                print('\
main.py\
This is the back end simulation service for the BPA platform. Once\
invoked, this program will run in an infinite loop until a fatal error\
occurs or the process is terminated. Simulation jobs, initiated by the\
BPA front end, will be spawned as seperate processes in parallel.\
Communication between the front and back ends is accomplished by a\
shared directory, see documentation for the file structure of this\
directory. This can be shared via an online platform such as Dropbox\
to enable online communication between front and back ends on\
different computers.\
\
Usage:\
./main.py -h\
./main.py [-d] path_to_shared_folder [dispatch_interval]\
\
-h, --help  - displays help text\
-d, --debug - service prints debug information to standard out,\
              and jobs print debug information to "[jobID].log"\
              in the job folder (../jobs/job_[jobID]).\
                ')
                sys.exit(0)
            elif arg=='-d' or arg=='--debug':
                b_debug=True
            else:
                print('main.py error: unknown command line option "'+arg+'"')
                sys.exit(1)
        else:
            # This is an argument.
            i_argCount=i_argCount+1
            if i_argCount==1:
                s_shareDir=arg
                if s_shareDir[-1]=='/': s_shareDir=s_shareDir[:-1]
            elif i_argCount==2:
                try:
                    r_interval=float(arg)
                except ValueError:
                    print('main.py error: interval argument is not a number')
                    sys.exit(1)
    if i_argCount<1 or i_argCount>2:
        print('main.py error: script accepts 1 or 2 argument(s)')
        sys.exit(1)

    # Main program.

    curDateTime=datetime.now()
    s_dateTime=curDateTime.strftime('%a %b %d %X %Y')
    if b_debug: print('main.py: SERVICE START @ '+s_dateTime)

    # Create dictionaries to hold all running processes and pipe connections.
    # They can be retrieved by job ID (string).
    dict_proc=dict()
    dict_pipe=dict()

    ### FUNCTION: killItWithFire
    # Kills a job with extreme prejudice. Sends a SIGKILL and erases the job directory.
    # This can be used if a job starts to look fishy.
    # Assumes that the job exists and is alive.
    def killItWithFire(s_jobID):
        proc=dict_proc[s_jobID]
        kill(proc.pid,signal.SIGKILL)
        rmtree(getJobDir(s_jobID))
        con,sender=dict_pipe[s_jobID]
        con.close()
        sender.close()
        del dict_proc[s_jobID]
        del dict_pipe[s_jobID]

    # Dispatch in infinite loop.
    i_failCount=0
    while True:
        curDateTime=datetime.now()
        s_dateTime=curDateTime.strftime('%a %b %d %X %Y')
        if b_debug: print('main.py: --------------------\nmain.py: starting dispatch @ '+s_dateTime)
        # Get current time, to time how long dispatch takes.
        start_time=time()

        # Get SQL database IP from file.
        f_SQL=open('.SQL.txt','r')
        s_SQLIP=f_SQL.readline().strip()
        s_SQLuser=f_SQL.readline().strip()
        s_SQLpwd=f_SQL.readline().strip()
        s_SQLdbs=f_SQL.readline().strip()
        s_errlog=f_SQL.readline().strip()
        f_SQL.close()
        if b_debug: print('main.py: connecting to SQL database at IP '+s_SQLIP)

        # Connect to SQL database.
        try:
            cnx=connector.connect(user=s_SQLuser,
                password=s_SQLpwd,
                host=s_SQLIP,
                database=s_SQLdbs,
                connection_timeout=r_interval)
        except:
            mainError('failed to connect to SQL database, skipping dispatch',s_errlog)
            sleepTilNext(start_time,r_interval,b_debug)
            continue
        cursor=cnx.cursor(buffered=True)

        ### FUNCTION: sql_update
        # Updates the sql table with a new "result" value.
        def sql_update(i_update,i_jobID):
            try:
                cursor.execute("UPDATE Calibro_results SET result = %d WHERE calproj_id = %d;" %(i_update,i_jobID))
                cnx.commit()
            except:
                mainError('failed to update SQL database',s_errlog)
            else:
                if b_debug: print('main.py: successfully updated the SQL database')

        # Check for required actions on jobs
        try:
            cursor.execute("SELECT t1.calproj_id,t1.name,t1.meas_perf,t2.result FROM Calibro_projects AS t1 JOIN Calibro_results AS t2 ON t1.calproj_id=t2.calproj_id;")
            query=cursor.fetchall()
        except:
            mainError('failed to query SQL database, skipping dispatch',s_errlog)
            sleepTilNext(start_time,r_interval,b_debug)
            continue
        else:
            if b_debug: print('main.py: successfully queried the SQL database')

        for (i_jobID,s_name,s_archive,i_progress) in query:

#            print(i_jobID,s_name,s_obs,s_bc,s_input,s_sim,i_progress)

            # Check stage of this job.
            s_jobID=str(i_jobID)
            i_update=-1

            if i_progress==0:
                # Start a job - python multiprocessing.
                # Check that a job with this ID doesn't already exist.
                if s_jobID in dict_proc:
                    print('main.py: job with ID '+s_jobID+' already exists, this job will not be started')
                    i_update=1
                    sql_update(i_update,i_jobID)
                    continue
                if b_debug:
                    print('main.py: *** starting new job ***')
                    print('main.py:   jobID - '+s_jobID)
                    print('main.py:   name - '+s_name)

                # Open a unidirectional pipe (slave->master) so the process can communicate its status.
                con,sender=Pipe(False)
                proc=Process(target=runJob,name='jobID_'+s_jobID,args=(s_jobID,s_name,s_archive,b_debug,sender,s_shareDir))
                proc.start()
                # Put the process and pipe connections into a dictionary for later retrieval.
                dict_proc[s_jobID]=proc
                dict_pipe[s_jobID]=(con,sender)
                i_update=1

            elif i_progress==1:

                # Check status of currently running job - python multiprocessing.

                if b_debug:
                    print('main.py: ### checking status of job ###')
                    print('main.py:   jobID - '+s_jobID)
                    print('main.py:   name - '+s_name)
                # Retrieve job and connection objects from dictionaries.
                if not s_jobID in dict_proc or not s_jobID in dict_pipe:
                    # Job says it is running, but it not registered.
                    # This probably means the service crashed and has been restarted.
                    # Restart the job ... unless there is a kill file in the job directory.
                    if b_debug: print('main.py:   jobID not registered')

                    if isfile(getJobDir(s_jobID)+'/kill.it'):
                        print('main.py: !!! kill file detected !!!')
                        i_update=9
                        sql_update(i_update,idx)
                        continue                        

                    if b_debug: print('main.py: *** restarting job ***')
                    if s_jobID in dict_proc: del dict_proc[s_jobID]
                    if s_jobID in dict_pipe: del dict_pipe[s_jobID]
                    con,sender=Pipe(False)
                    proc=Process(target=runJob,name='jobID_'+s_jobID,args=(s_jobID,s_name,s_archive,b_debug,sender,s_shareDir))
                    proc.start()
                    # Put the process and pipe connections into a dictionary for later retrieval.
                    dict_proc[s_jobID]=proc
                    dict_pipe[s_jobID]=(con,sender)                    
                    continue

            # Check for an admin kill command (a file called "kill.it" in the job directory).
                if isfile(getJobDir(s_jobID)+'/kill.it'):
                    print('main.py: !!! kill file detected !!!')
                    killItWithFire(s_jobID)
                    i_update=9
                    sql_update(i_update,idx)
                    continue

                proc=dict_proc[s_jobID]
                con,sender=dict_pipe[s_jobID]
                if proc.is_alive():
                    # Job is still alive, check its status.
                    i_tmp=0
                    b_done=False
                    if b_debug: print('main.py:   job is alive')
                    while con.poll():
                        i_tmp=con.recv()
                        if not type(i_tmp)==int:
                            # Unexpected signal type.
                            i_tmp=None
                            break
                        if b_debug: print('main.py:   job gave signal "'+str(i_tmp)+'"')
                        if i_tmp==4:
                            b_done=True
                    if b_done:                        
                        # Exit signal recieved but job is still running.
                        # Wait half a second then check again.
                        sleep(0.5)
                        if proc.is_alive():
                            # This shouldn't really be possible, so something odd is going on.
                            # Kill the job just to be safe.
                            # TODO - maybe check outputs?
                            killItWithFire(s_jobID)
                            if b_debug: print('main.py: !!! job looked dodgy !!!')
                            i_update=9
                        elif proc.exitcode==0:
                            if i_tmp==4:
                                # Check if the performance flag is still in the pipe.
                                if con.poll():
                                    i_tmp=con.recv()
                                else:
                                    # This shouldn't be possible.
                                    if b_debug: print('main.py: !!! job didn\'t give performance flag !!!')
                                    i_update=9
                            if i_tmp==0: 
                                i_update=3 # compliant
                            elif i_tmp==1: 
                                i_update=5
                            elif i_tmp==2: 
                                i_update=4
                            elif i_tmp==3: 
                                i_update=6
                            else:
                                # Unexpected performance flag.
                                # Again, this shouldn't really be possible.
                                if b_debug: print('main.py: !!! job gave unrecognised performance flag !!!')
                                i_update=9
                        else:
                            i_update=2
                        # Close pipe and remove dictionary entries.
                        sender.close()
                        con.close()
                        del dict_proc[s_jobID]
                        del dict_pipe[s_jobID]
                    elif i_tmp==0:
                        i_update=1
                    elif i_tmp==1:
                        i_update=1
                    elif i_tmp==2:
                        i_update=1
                    elif i_tmp==3:
                        i_update=1
                    else:
                        # Unexpected signal from job - kill it with fire!
                        killItWithFire(s_jobID)
                        if b_debug: print('main.py: !!! job looked dodgy !!!')
                        i_update=9
                else:
                    # Job is dead, check exit code and make sure it gave the expected exit signal.
                    sender.close()
                    i_tmp=0
                    b_done=False
                    if b_debug: print('main.py:   job is dead')
                    while con.poll():
                        try: 
                            i_tmp_prev=i_tmp
                            i_tmp=con.recv()
                        except EOFError:
                            # Trap this error to avoid crashing the service.
                            i_tmp=i_tmp_prev
                            break
                        if b_debug: print('main.py:   job gave signal "'+str(i_tmp)+'"')
                        if i_tmp==4:
                            b_done=True
                    con.close()
                    if b_done and proc.exitcode==0:                        
                        if i_tmp==0: 
                            i_update=3
                        elif i_tmp==1: 
                            i_update=5
                        elif i_tmp==2: 
                            i_update=4
                        elif i_tmp==3: 
                            i_update=6
                        else:
                            # Unexpected performance flag.
                            # This shouldn't really be possible.
                            if b_debug: print('main.py: !!! job gave unrecognised performance flag !!!')
                            i_update=9
                    elif proc.exitcode!=0:
                        i_update=2
                    else:
                        if b_debug: print('main.py: !!! job didn\'t give exit signal !!!')
                        i_update=9
                    # Remove dictionary entries.
                    del dict_proc[s_jobID]
                    del dict_pipe[s_jobID]

                if i_update==3 or i_update==4 or i_update==5 or i_update==6:
                    if b_debug: print('main.py: *** job complete ***')

                elif i_update==1:
                    if b_debug: print('main.py: *** job still in progress ***')

                elif i_update==2:
                    if b_debug: print('main.py: !!! job failed !!!')

            elif i_progress==7:
                # Cancel a job.
                if b_debug:
                    print('main.py: ### cancelling job ###')
                    print('main.py:   jobID - '+s_jobID)
                    print('main.py:   name - '+s_name)
                # Retrieve job and connection objects from dictionaries.
                if not s_jobID in dict_proc or not s_jobID in dict_pipe:
                    if b_debug: print('main.py:   jobID not registered')
                    if b_debug: print('main.py: *** non-existent job flagged as cancelled ***')
                    i_update=8
                    sql_update(i_update,i_jobID)
                    continue
                proc=dict_proc[s_jobID]
                con,sender=dict_pipe[s_jobID]
                if proc.is_alive():
                    # Job is still alive, terminate it.
                    if b_debug: print('main.py:   job is alive')
                    proc.terminate()
                    slept=0
                    b_zombie=False
                    while proc.is_alive():
                        sleep(0.1)
                        slept+=1
                        if slept>50:
                            mainError('process BPAsim'+s_jobID+' left zombified',s_errlog)
                            b_zombie=True
                            break
                    if not b_zombie: proc.join()
                    sender.close()
                    con.close()
                    del dict_proc[s_jobID]
                    del dict_pipe[s_jobID]
                    i_update=8
                    if b_debug: print('main.py: *** job cancelled ***')
                else:
                    # Job is dead, check exit code and make sure it gave the expected exit signal.
                    sender.close()
                    i_tmp=0
                    b_done=False
                    if b_debug: print('main.py:   job is dead')
                    while con.poll():
                        try: 
                            i_tmp_prev=i_tmp
                            i_tmp=con.recv()
                        except EOFError:
                            # Trap this error to avoid crashing the service.
                            i_tmp=i_tmp_prev
                            break
                        if b_debug: print('main.py:   job gave signal "'+str(i_tmp)+'"')
                        if i_tmp==4:
                            b_done=True
                    con.close()
                    if b_done and proc.exitcode==0:
                        if i_tmp==0: 
                            i_update=3
                        elif i_tmp==1: 
                            i_update=5
                        elif i_tmp==2: 
                            i_update=4
                        elif i_tmp==3: 
                            i_update=6
                        else:
                            # Unexpected performance flag.
                            # This shouldn't really be possible.
                            if b_debug: print('main.py: !!! job gave unrecognised performance flag !!!')
                            i_update=9
                    elif proc.exitcode!=0:
                        i_update=2
                    else:
                        if b_debug: print('main.py: !!! job didn\'t give exit signal !!!')
                        i_update=9
                    # Remove dictionary entries.
                    del dict_proc[s_jobID]
                    del dict_pipe[s_jobID]

                    if i_update==3 or i_update==4 or i_update==5 or i_update==6:
                        if b_debug: print('main.py: *** job complete ***')

                    elif i_update==2:
                        if b_debug: print('main.py: !!! job failed !!!')
            
            # Update sql database with new job status.
            if i_update>0:
                sql_update(i_update,i_jobID)

        cnx.close()

        # Check that dispatch has not been running for longer than the interval.
        sleepTilNext(start_time,r_interval,b_debug)

if __name__=='__main__': main()
