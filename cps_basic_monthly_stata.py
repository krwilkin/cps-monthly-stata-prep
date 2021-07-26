# -*- coding: utf-8 -*-
"""
python script to work with basic monthly cps files. namely, it creates
stata 'infix' dictionary files by processing the data dictionaries for
each set of cps files (i.e., those before 2017 and those including and after)

Created on Sun Mar 18 11:54:59 2018
updated Jan 30 2019

@author: krw (krwilkin@gmail.com)
"""

# import modules
import os
import re
import zipfile
import shutil

try:
    import urllib2
except:
    import urllib.request


# declare working directory
'''
NOTE: assumes you want to download data in the current directory for your
      python installation. if you want a different directory, then replace
      'os.getcwd()' in the following definition of 'localdir'
'''
rootdir = os.getcwd()
localdir = os.path.join( rootdir , 'cpsbm' )

if not os.path.exists( localdir ):
    os.makedirs( localdir )

'''
# access web page and download data files
'''
# declare url and open it
cps_url = 'https://thedataweb.rm.census.gov/ftp/cps_ftp.html'

try:
    response = urllib2.urlopen( cps_url )
    html = response.read()
except:
    request = urllib.request.Request( cps_url )
    response = urllib.request.urlopen( request )
    html = response.read().decode()

response.close()

# set regular expression for select zip file names and store urls in a list
re_find = 'http.*basic{1}.*[a-z]{3}1[1-9]pub.zip'

files = re.findall( re_find , html )

# set regular expression to store dictionary file urls
'''
NOTE: there are two sets: (1) recent versions that have 'Record_Layout' in the
      file name and (2) legacy versions with two-digit year and 'dd' in their
      file name.
'''
re_find = 'http.*basic/201[0-9]{1,}.*\.txt'

ddf = re.findall( re_find , html )


# download files not already downloaded to disk
'''
NOTE: the following first creates a list of any files within 'localdir'
      then only downloads files from 'files' not already on disk ('d_files')
'''
d_files = os.listdir( localdir )

for i in files + ddf:
    f = i.split( '/' )[ -1 ]
    if f not in d_files:
        print( 'downloading' , f )
        fh = os.path.join( localdir , f )
        try:
            # with urllib2.urlopen( i ) as response , open( fh , 'wb') as outf:
            #     shutil.copyfileobj( response, outf )
            with open( os.path.join( localdir , f ) , 'wb' ) as outf:
                outf.write( urllib2.urlopen( i ).read() )
                outf.close()
        except:            
            with urllib.request.urlopen( i ) as response , open( fh , 'wb') as outf:
                shutil.copyfileobj( response, outf )


# PROCESS CPS DATA DICTIONARY FILES TO CREATE STATA .DCT FILES (INFIX DICTIONARY)
'''
NOTES: the list 'keep_vars' is a master list of variables you wish to retain
       from the raw cps files; it's not sensible to retain all variables
       as they are not needed and doing so would eat disk space. if you want to
       additoinal variables, add them to this list. in addition, the stata
       infix dictionary will specify character variables as strings instead
       of numbers if they are added to the 'str_vars' list; note that all
       variables in the 'str_vars' list should also be in 'keep_vars' (this is
       something that can be relaxed, but i don't code for it here). i also 
       attempted to account for implied decimals in the data (e.g., 'dec4_vars'
       and 'dec2_vars') but the infix dictionary did not like it. instead, i
       add decimals in a separate stata routine. the raw cps data do not 
       include the decimals, but they are 'implied'
'''
# define set of cps keep variables
keep_vars = [ 'HRHHID' , 'HRMONTH' , 'HRYEAR4' , 'HURESPLI' , 'HRHHID2' ,
              'HUFINAL' , 'HRHTYPE' , 'HRMIS' , 'HUINTTYP' , 'HRLONGLK' ,
              'GEREG' , 'GEDIV' , 'GESTFIPS' , 'GTCBSA' , 'GTCBSASZ' ,
              'PRCIVLF' , 'PREXPLF' , 'PUIODP1' , 'PEIO1COW' , 'PEIO2COW' ,
              'PEERNHRO' , 'PEERNLAB' , 'PEERNCOV' , 'PESCHENR' , 'PESCHFT' ,
              'PWLGWGT' , 'PWORWGT' , 'PWSSWGT' , 'PERRP' , 'PRTAGE' , 
              'PRTFAGE' , 'PEMARITL' , 'PESPOUSE' , 'PESEX' , 'PEEDUCA' ,
              'PTDTRACE' , 'PRDTHSP' , 'PRCITSHP' , 'PEMLR' , 'PUDIS' , 
              'PEMJOT' , 'PEMJNUM' , 'PEHRUSL1' , 'PEHRUSL2' , 'PEHRUSLT' ,
              'PEHRRSN1' , 'PEHRRSN2' , 'PEHRRSN3' , 'PUHROT1' , 'PUHROT2' ,
              'PEJHRSN' , 'PEJHWKO' , 'PRCHLD' , 'PRNMCHLD' , 'PXHRUSL1' ,
              'PXHRUSL2' , 'PXHRUSLT' , 'PXJHWKO' , 'PEIO1ICD' , 'PEIO2ICD' ,
              'PEIO1OCD' , 'PEIO2OCD' , 'PXJHRSN' , 'PXIO1ICD' , 'PXIO2ICD' ,
              'PXIO1OCD' , 'PXIO2OCD' , 'QSTNUM' , 'OCCURNUM' , 'PRERNWA' ,
              'PECERT1' , 'PECERT2' , 'PECERT3' , 'PUSLFPRX' , 'PRFTLF' ,
              'PRHRUSL' , 'PRPTHRS' , 'PRPTREA' , 'PREMPHRS' , 'PULINENO' ,
              'PRDTIND1' , 'PRDTIND2' , 'PRDTOCC1' , 'PRDTOCC2' , 'PRAGNA' ,
              'PRDTCOW1' , 'PRDTCOW2' , 'PTHR' , 'PRERELG' , 'PEERNUOT' ,
              'PEERNPER' , 'PEERNRT' , 'PEERNHRY' , 'PUERNH1C' , 'PEERNH2' ,
              'PEERNH1O' , 'PRERNHLY' , 'PEERN' , 'PEERNHRO' , 'PTWK' ,
              'PUERN2' , 'PTOT' , 'PEERNWKP' , 'PRWERNAL' , 'PRHERNAL' , 'PEAGE' ,
			    'PEPDEMP1' , 'PEPDEMP2' , 'PTNMEMP1' , 'PTNMEMP2' , 'HUPRSCNT	' ]
              

dec4_vars = [ 'PWLGWGT' , 'PWORWGT' , 'PWSSWGT' ]

dec2_vars = [ 'PRERNWA' ]

str_vars = [ 'HRHHID' , 'HRHHID2' , 'GESTFIPS' , 'GTCBSA' ]


# get record information from each record layout; store in list
# NOTE: keep only those variables in 'keep_vars'
re_patt = '1\d{1}'

rec_lst = []
rec_dct = []
rec_out = []

for i in ddf:
    year = re.findall( re_patt , i.split( '/' )[ -1 ] )
    
    if len( year ) == 1:
        if year[ 0 ] == '10':
            y = '11'
        else:
            y = year[ 0 ]
    
    else:
        continue
    
    rec_lst.append( 'rec_lst_20{0}'.format( y ) )
    rec_dct.append( 'rec_dct_20{0}'.format( y ) )
    rec_out.append( 'cps20{0}.dct'.format( y ) )


for i , j in zip( rec_lst, rec_dct ):
    locals()[ i ] = []
    locals()[ j ] = {}


for i , j in zip( ddf , rec_lst ):
    with open( os.path.join( localdir , i.split( '/' )[ -1 ] ) , 'r' ) as f:
        for line in f.readlines():
            ls = line.split()
            if len( ls ) > 0:
                if ls[ 0 ] in keep_vars:
                    re_dig = re.findall( '[0-9]' , ls[ 1 ] )
                    if len( re_dig ) > 0:
                        l = line.split( '\r\n' )[ 0 ]
                        locals()[ j ].append( l )


# populate python dictionary for each cps record layout
for d , j in zip( rec_lst , rec_dct ):
    for i in locals()[ d ]:
        v = i.split()[ 0 ]
        l = i.split( v )[ -1 ]
        
        n = re.findall( '[0-9]+' , l )
        
        l_v = n[ 0 ]
        st = n[ -2 ]
        en = n[ -1 ]
        
        ls = [ l_v , st , en ]
        
        locals()[ j ][ v ] = [ int( x ) for x in ls ]



# write .dct file
for i , j in zip( rec_out , rec_dct ):
    with open( os.path.join( localdir , i ) , 'w' ) as f:
        
        text = 'infix dictionary {\n'
        f.write( text )
        
        in_d = locals()[ j ]
        
        for r in sorted( in_d.items() , key = lambda in_d : in_d[ 1 ][ 1 ] ):
            v = r[ 0 ]
            ls = r[ -1 ]
            
            len_v = ls[ 0 ]
            st = ls[ 1 ]
            en = ls[ -1 ]
            
            if v in str_vars:
                text = '    str {0} {1}-{2}\n'.format( v , st , en )
                f.write( text )
            else:
                text = '    {0} {1}-{2}\n'.format( v , st , en )
                f.write( text )
        
        f.write( '}\n' )
        
        del in_d


# EXTRACT CONTENTS OF DOWNLOADED .ZIP FILES SO THEY CAN BE IMPORTED INTO STATA
# extract zip files
z_files = []

for i in files:
    if i.split( '.' )[ -1 ] == 'zip':
	    z_files.append( i.split( '/' )[ -1 ] )

for i in z_files:
    z = open( os.path.join( localdir , i ) , 'rb' )
    zf = zipfile.ZipFile( z )
    for name in zf.namelist():
        if name not in d_files:
            print( 'extracting' , name )
            zf.extract( name , localdir )
    z.close()

# clean up zip files
for i in z_files:
    z = os.remove( os.path.join( localdir , i ) )

