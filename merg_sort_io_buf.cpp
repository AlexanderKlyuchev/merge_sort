#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <queue>
#include <fstream>

using namespace std;

#define DEBUG 0

#if DEBUG
static const string cur_path="/Users/aleksandrklucev/external_sort/external_sort/";
#else
static const string cur_path=string();
#endif

static const string tmp_file_prefix=cur_path+"tmp_file_";
static const string tmp_file_prefix2="tmp";

enum log_level{
    info=0,
    error
};

typedef unsigned long long long_long;


#define debug_print(level, fmt, ...) \
do { if (DEBUG || level > info) fprintf(stderr, "%s:%d:%s(): \n" fmt, __FILE__, \
__LINE__, __func__, __VA_ARGS__); } while (0)


template<class T>
class BufferedFileReader{
    static const int BUF_SIZE=2048;
public:
    BufferedFileReader(string file_name, int num_space = 0 ,int num_chunks=0){

        m_infile.open(file_name, ifstream::in);
        m_file_name=file_name;
        m_empty=false;
        
        if(m_infile){
            long_long buffer_size;
            
            if(num_space && !num_chunks){
                estimateBestSizeOfBlocks(num_space);
                buffer_size=getEstimatedBlockSize();
            }else if(num_space && num_chunks){
                buffer_size=num_space/num_chunks;
            }else{
                buffer_size=BUF_SIZE;
            }
            
            m_data_buffer.reserve(buffer_size);
            m_infile.rdbuf()->pubsetbuf(&m_data_buffer.front(), buffer_size);
            reload();
        }else{
            cout<<file_name<<" file not found"<<endl;
        }
    }
    
    void reload(){
        if(getline(m_infile, m_cache)){
            m_empty=false;
        }else{
            m_empty=true;
            m_cache=T();
        }
    }
    
    T read_line(){
        T line=m_cache;
        reload();
        return line;
    }
    
    bool is_empty() const{
        return m_empty;
    }
    
    T peek() const{
        return m_cache;
    }
    
    void pop(){
        reload();
    }
    
    void close(){
        if(m_infile)
            m_infile.close();
    }
    
    void remove_file(){
        remove(m_file_name.c_str());
    }
    
    long_long getEstimatedBlockSize() const{
        return m_blocksize;
    }
    
    void estimateBestSizeOfBlocks(int num_space){
        std::streampos sizeOfFile=0;
        long blocksize=0;
        static const int MAXTEMPFILES = 1024;
        if(m_infile){
            
            std::streampos cur_pos=m_infile.tellg();
            
            m_infile.seekg(0, m_infile.end);
            sizeOfFile=m_infile.tellg();

            blocksize = sizeOfFile / MAXTEMPFILES ;
            
            if( blocksize < num_space/2)
                blocksize = num_space/2;
            else {
                if(blocksize >= num_space)
                    cout<<"We expect to run out of memory. "<<endl;
            }
            
            cout<<"file size "<<sizeOfFile<<endl;
            cout<<"block size "<<blocksize<<endl;
            m_infile.seekg(cur_pos, m_infile.beg);
        }
        
        m_blocksize=blocksize;
    }
    
private:
    vector<char> m_data_buffer;
    ifstream m_infile;
    string m_file_name;
    T m_cache;
    bool m_empty;
    long_long m_blocksize;
};


typedef shared_ptr<BufferedFileReader<string> > BufferedFileReader_ptr;


bool operator<(const BufferedFileReader<string>& reader1, const BufferedFileReader<string>& reader2){
    return reader1.peek() < reader2.peek();
}

bool operator>(const BufferedFileReader<string>& reader1, const BufferedFileReader<string>& reader2){
    return reader1.peek() > reader2.peek();
}

bool operator>(BufferedFileReader_ptr reader1, BufferedFileReader_ptr reader2){
    return reader1->peek() > reader2->peek();
}


class BufferedFileWritter{
    static const int BUF_SIZE=2048;
    
public:
    BufferedFileWritter(string filename, long  buffer_size = BUF_SIZE){
        m_outfile.open(filename);
        
        if(m_outfile){
            m_data_buffer.reserve(buffer_size);
            m_outfile.rdbuf()->pubsetbuf(&m_data_buffer.front(), buffer_size);
        }else{
            cout<<filename<<" file not found"<<endl;
        }
    }
    
    void write(const char* str){
        if(m_outfile)
            m_outfile.write(str, strlen(str));
    }
    
    void new_line(){
        if(m_outfile)
            m_outfile.write("\n", strlen("\n"));
    }

private:
    vector<char> m_data_buffer;
    ofstream m_outfile;
    
};


vector<string> split_file(const char* file_name, int num_space){
    
    BufferedFileReader<string> reader(file_name, num_space);
    long_long blocksize=reader.getEstimatedBlockSize();
    vector<string> tmp_files;
    string line;
    int num_files=0;
    do {
        long currentblocksize = 0;
        vector<string> tmplist;
        while((currentblocksize < blocksize)
              && !reader.is_empty()){
            line = reader.read_line();
            tmplist.push_back(line);
            currentblocksize += line.size();
        }
        
        sort(tmplist.begin(), tmplist.end());
        stringstream tmpfile;
        tmpfile<<tmp_file_prefix<<num_files++<<".txt";
    
        BufferedFileWritter wreater(tmpfile.str(), blocksize);
        for(vector<string>::const_iterator it=tmplist.begin(); it!= tmplist.end(); it++){
            wreater.write((*it).c_str());
            wreater.new_line();
        }
        
        tmp_files.push_back(tmpfile.str());
        tmplist.clear();
    }while(!reader.is_empty());
    
    return tmp_files;

}


void merge_sorted_files(const vector<string>& list_of_chunks, const string& out_file_name, int num_space){
    priority_queue<BufferedFileReader_ptr, vector<BufferedFileReader_ptr >, greater<BufferedFileReader_ptr> >  pq;
    
    int num_space_for_write_buffer = num_space/(list_of_chunks.size()+1);
    int num_space_for_read_buffers = num_space - num_space_for_write_buffer;
    
    for(vector<string>::const_iterator it=list_of_chunks.begin();  it!=list_of_chunks.end(); it++){
        BufferedFileReader_ptr reader( new BufferedFileReader<string>(*it, num_space_for_read_buffers,1));
        pq.push(reader);
    }
    
    BufferedFileWritter writter(out_file_name, num_space_for_write_buffer);
    while(pq.size()>0) {
        BufferedFileReader_ptr rd=pq.top();
        
        string r=rd->peek();
        rd->pop();
        pq.pop();
        writter.write(r.c_str());
        writter.new_line();
        
        if(rd->is_empty()){
            rd->close();
            rd->remove_file();
        }else{
            pq.push(rd);
        }
        
    }
    
}


int main()
{
    int num_space=8;
    string inputfile;
    string outfile;
    
    {
    int space;
    cout<<"enter num of space"<<endl;
    cin>>space;
    if(space > 0)
        num_space=space;
    else
        debug_print(error, "num sapce must be > 0 use default value %d", num_space);
    cout<<endl;
    
    }
    
    {
    string filename;
    cout<<"enter src file "<<endl;
    cin>>filename;
    cout<<endl;
    inputfile=cur_path+filename;
    }
    
    {
    string filename;
    cout<<"enter dst file "<<endl;
    cin>>filename;
    cout<<endl;
    outfile=cur_path+filename;
    }
    
    
    vector<string>  lis_of_chunks=split_file(inputfile.c_str(), num_space);
    merge_sorted_files(lis_of_chunks, outfile , num_space);
    
}

