#include <stxxl/stats>
#include <stxxl/parallel_sorter_synchron>
#include <chrono>
#include <limits>
#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <algorithm>

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
		for(size_t i = static_cast<size_t>(1)<<27; i>0; i--)
		{
			quartetSorter.push(i, tid);
		}
	}		
	
	std::cout << (stxxl::stats_data(*Stats) - stats_begin);
	
    quartetSorter.sort();  // sort elements (in ascending order)
	
	quartetSorter.clear();
	
	return 0;
}

