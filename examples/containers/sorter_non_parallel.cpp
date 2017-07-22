/***************************************************************************
 *  examples/containers/sorter1.cpp
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2013 Daniel Feist <daniel.feist@student.kit.edu>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

//! [example]
#include <stxxl/sorter>
#include <stxxl/stats>
#include <stxxl/timer>
#include <limits>
#include <iostream>
#include <fstream>

struct my_comparator
{
    bool operator () (const int& a, const int& b) const
    {
        return a < b;
    }

    int min_value() const
    {
        return 0;
    }

    int max_value() const
    {
        return std::numeric_limits<int>::max();
    }
};

int main()
{
    ofstream result;
    result.open ("result_non_parallel.dat");
   std::ifstream ifs("testfile.dat");
	std::int line;

// template parameter <ValueType, CompareType, BlockSize, AllocStr(optional)>
    typedef stxxl::sorter<int64_t, my_comparator> sorter_type;

    // create sorter object (CompareType(), MainMemoryLimit)
    sorter_type int_sorter(my_comparator(), static_cast<int64_t>(1)<<30);

	std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    // fill sorter with elements order in descending order
    //for (int i = 1000; i > 0; i--)
   // {
    //    int_sorter.push(i);
    //}
	while(std::getline(ifs, line))
{
		int_sorter.push(line);
		std::cout << "/n" << line << std::endl;
}

    int_sorter.sort();  // sort elements (in ascending order)

    // walk through sorted values and print them out
    while (!int_sorter.empty())
    {
        result << *int_sorter << " ;";
        ++int_sorter;
    }
    
	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	std::cout << "/n It took: " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count()
				<< " microseconds." << std::endl;
    result.close();
    input.close();
    return 0;
}
//! [example]
