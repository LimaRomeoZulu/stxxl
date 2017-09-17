#include <stxxl/stats>
#include <stxxl/parallel_sorter_synchron>
#include <stxxl/vector>
#include <stxxl/io>
#include <chrono>
#include <limits>
#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <algorithm>
#include <stxxl/bits/common/uint_types.h>

struct my_comparator1
{
   bool operator () (const size_t& a, const size_t& b) const
   {
       return a < b;
   }

   size_t min_value() const
   {
       return std::numeric_limits<size_t>::min();
   }

   size_t max_value() const
   {
       return std::numeric_limits<size_t>::max();
   }
};


int main(int argc, char** argv) {
	
	
    stxxl::syscall_file input_file("numbers1.dat", stxxl::file::RDONLY | stxxl::file::DIRECT);
    typedef stxxl::vector<size_t> vector_type;
    vector_type input_vector(&input_file);
	
    // output file object
    stxxl::syscall_file output_file("output.dat", stxxl::file::RDWR | stxxl::file::CREAT | stxxl::file::DIRECT);
	vector_type output_vector(&output_file);
	
	stxxl::stats * Stats = stxxl::stats::get_instance();
	
	stxxl::stats_data stats_begin(*Stats);
	
	typedef stxxl::parallel_sorter_synchron<size_t, my_comparator1 > sorter_type;
	int nthread;
	std::stringstream(argv[1]) >> nthread;
	sorter_type quartetSorter(my_comparator1(),static_cast<size_t>(1)<<26, nthread);


	#pragma omp parallel num_threads(nthread)
	{		
		const int tid = omp_get_thread_num();
		#pragma omp for 
		for(size_t i = 0; i < input_vector.size(); i++)
		{
			quartetSorter.push(i, tid);
		}
	}		
	
	std::cout << (stxxl::stats_data(*Stats) - stats_begin);
	
	omp_set_nested(1); 
	
    quartetSorter.sort();  // sort elements (in ascending order)
	
	omp_set_nested(0);
	
	//while (!quartetSorter.empty())
	//{
		//output_vector.push_back(*quartetSorter);
		//++quartetSorter;
	//}
	 
	quartetSorter.clear();
	
	return 0;
}

