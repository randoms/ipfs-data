ipfs-data
==========

file system base on ipfs, you can read, write, and change files in it.

ipfs has a build in file system, however you can only read file from it. And the speed is extremely slow, only about 2MB/S.
This is a much better fs for ipfs. The read speed could reach 50MB/S. More than just that, you can use it as a normal file system. Create, delete directories. Copy and paste, create, delete and edit files.

## install dependencies
### install mongodb

just follow the instructions here

    https://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/

### install python dependencies

    pip install pymongo fusepy requests

### install ipfs

just follow the instructions here

    https://ipfs.io/docs/install/

## How to use

Download the source code, if you want to mount to /data just run the code below

    python ipdatafs.py /data
    
It works like a normal file system. But all the files are stored in ipfs. You can view your file through browser directly.

## How it works
ipfs is a p2p file system. It is content addressed, so if you changed the content of file, you have to add the whole file to ipfs again. That's why you cannot change files in the build in fs. The process to change a file is to delete the old file and create a new one to replace it. This program automates the process. The infomation of files and folders are stored in database. So when we edit a file we just replace the ipfs address of the file and the files are remain the same to users.


这个一个基于ipfs的文件系统。ipfs自带了一个文件系统，但是做的很粗糙。自带的文件系统是只读的，而且读取速度非常慢，速度只有2MB/S。我想用ipfs来备份自己的文件，自带的文件系统不好用，于是自己又重新写了一个。这个文件系统可以像其他文件系统一样用。你可以创建，删除修改文件和文件夹。读取速度能达到50MB/S左右。

## 安装
安装mongodb
按照这里的指示就可以了

    https://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/

安装python的相关依赖包

    pip install pymongo fusepy requests

安装ipfs
按照这里的指示就可以了

    https://ipfs.io/docs/install/

## 使用
下载这个源代码。比如你想挂载到/data文件夹下，执行
    
    python ipdatafs.py /data
    
就可以了。你可以像普通的文件系统一样使用它，不过里面的文件全部都被存储在ipfs内。你可以通过浏览器直接访问文件内容。

## 实现原理
ipfs是一个内容地址网络。也就是不同内容的文件对应的地址完全不同。所以即使你只改了文件中的一个字也要重新把整个文件添加进ipfs网络内。这也是自带的文件系统做成只读的原因。更改文件的过程实际上就是把就的文件删除，重新添加新的文件的过程。这个程序就是把这个过程自动化了。同时文件和文件夹的信息单独用数据库存起来。这样更改文件的时候只是把文件对应的ipfs地址改掉，用户看起来还是原来的那个文件。
