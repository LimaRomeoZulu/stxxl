#ifndef STABLE_KSORT_HEADER
#define STABLE_KSORT_HEADER
/***************************************************************************
 *            stable_ksort.h
 *
 *  Tue Feb  4 16:45:46 2003
 *  Copyright  2003  Roman Dementiev
 *  dementiev@mpi-sb.mpg.de
 ****************************************************************************/


// it is a first try: distribution sort without sampling
// I rework the stable_ksort when I would have a time

#include <math.h>
#include "../mng/mng.h"
#include "../common/utils.h"
#include "../mng/buf_istream.h"
#include "../mng/buf_ostream.h"
#include "../common/simple_vector.h"
#include "intksort.h"

__STXXL_BEGIN_NAMESPACE

//! \addtogroup stlalgo
//! \{

/*! \internal
*/
namespace stable_ksort_local
{
  template <class type_,class type_key>
  void classify_block(type_ * begin,type_ * end,type_key * & out,int * bucket,unsigned offset, unsigned shift)
  {
    for (type_ * p = begin;p<end; p++,out++)	// count & create references
    {
      out->ptr = p;
      typename type_::key_type key = p->key();
      int ibucket = (key - offset) >> shift;
      out->key = key;
      bucket[ibucket]++;
    }
  }

	template <typename type>
	struct type_key
	{
		typedef typename type::key_type key_type;
		key_type key;
		type * ptr;
		
		type_key() {};
		type_key(key_type k, type * p):key (k), ptr (p)
		{
		};
	};
	
	template <typename type>
	bool operator  < (const type_key<type> & a, const type_key<type> & b)
	{
			return a.key < b.key;
	}
	
	template <typename type>
	bool operator  > (const type_key<type> & a, const type_key<type> & b)
	{
			return a.key > b.key;
	}
	

	template <typename BIDType_,typename AllocStrategy_>
	class bid_sequence
	{
	public:
		typedef BIDType_ bid_type;
		typedef bid_type & reference;
		typedef AllocStrategy_ alloc_strategy;
		typedef typename simple_vector<bid_type>::size_type size_type;
		typedef typename simple_vector<bid_type>::iterator iterator;
	protected:
		simple_vector<bid_type> * bids;
		alloc_strategy alloc_strategy_;
		
		bid_sequence() {}
	public:
		bid_sequence(size_type size_)
		{
			bids = new simple_vector<bid_type>(size_);
			block_manager * mng = block_manager::get_instance();
			mng->new_blocks(alloc_strategy_,bids->begin(),bids->end());
		}
		reference operator [] (size_type i)
		{
			size_type size_ = size(); // cache size in a register
			if(i < size_)
				return *(bids->begin() + i);
			
			block_manager * mng = block_manager::get_instance();
			simple_vector<bid_type> * larger_bids = new simple_vector<bid_type>((i+1)*2);
			std::copy(bids->begin(),bids->end(),larger_bids->begin());
			mng->new_blocks(alloc_strategy_,larger_bids->begin() + size_,larger_bids->end());
			delete bids;
			bids = larger_bids;
			return *(larger_bids->begin() + i);
		}
		size_type size() { return bids->size();} 
		iterator begin() { return bids->begin(); }
		~bid_sequence()
		{
			block_manager::get_instance()->delete_blocks(bids->begin(),bids->end());
			delete bids;
		}
	};

	template <typename ExtIterator_>
	void distribute(
		bid_sequence<typename ExtIterator_::vector_type::block_type::bid_type,
			typename ExtIterator_::vector_type::alloc_strategy> * bucket_bids,
		int64 * bucket_sizes,
		const int nbuckets,
		const int lognbuckets,
		ExtIterator_ first,
		ExtIterator_ last,
		const int nread_buffers,
		const int nwrite_buffers)
	{
		typedef typename ExtIterator_::vector_type::value_type value_type;
		typedef typename value_type::key_type key_type;
		typedef typename ExtIterator_::block_type block_type;
		typedef typename block_type::bid_type bid_type;
		typedef buf_istream<typename ExtIterator_::block_type,
				typename ExtIterator_::bids_container_iterator> buf_istream_type;
	
		int i=0;
		
		buf_istream_type in(first.bid(),last.bid() + ((first.block_offset())?1:0),
			nread_buffers);
		
		buffered_writer<block_type> out(
			nbuckets + nwrite_buffers,
			nwrite_buffers);
		
		unsigned int * bucket_block_offsets = new unsigned int[nbuckets];
		unsigned int * bucket_iblock = new unsigned int[nbuckets];
		block_type ** bucket_blocks = new block_type *[nbuckets];
		
		std::fill(bucket_sizes,bucket_sizes + nbuckets,0);
		std::fill(bucket_iblock,bucket_iblock + nbuckets,0);
		std::fill(bucket_block_offsets,bucket_block_offsets + nbuckets,0);
		
		for(i= 0; i< nbuckets; i++)
			bucket_blocks[i] = out.get_free_block();
		
		ExtIterator_ cur = first - first.block_offset();
		
		// skip part of the block before first untouched
		for( ;cur != first;cur++)
			++in;
		
		const int shift = sizeof(key_type)*8 - lognbuckets;
		// search in the the range [_begin,_end)
		for( ;cur != last;cur++)
		{
			key_type cur_key = in.current().key();
			int ibucket = cur_key >> shift;
			
			int block_offset = bucket_block_offsets[ibucket];
			in >> (bucket_blocks[ibucket]->elem[block_offset++]);
			if(block_offset == block_type::size)
			{
				block_offset = 0;
				int iblock = bucket_iblock[ibucket]++;
				bucket_blocks[ibucket] = out.write(bucket_blocks[ibucket],bucket_bids[ibucket][iblock]);
			}
			bucket_block_offsets[ibucket] = block_offset;
		}
		for(i = 0 ;i<nbuckets;i++)
		{
			if(bucket_block_offsets[i])
			{
				out.write(bucket_blocks[i],bucket_bids[i][bucket_iblock[i]]);
			}
			bucket_sizes[i] = int64(block_type::size) * bucket_iblock[i] + 
				bucket_block_offsets[i];
			STXXL_MSG("Bucket "<<i<<" has size "<<bucket_sizes[i]<<
				", estimated size: "<<((last-first)/int64(nbuckets)))
		}
		
		
		delete [] bucket_blocks;
		delete [] bucket_block_offsets;
		delete [] bucket_iblock;
	}
};

template <typename ExtIterator_>
void stable_ksort(ExtIterator_ first, ExtIterator_ last,unsigned M)
{
	typedef typename ExtIterator_::vector_type::value_type value_type;
	typedef typename value_type::key_type key_type;
	typedef typename ExtIterator_::block_type block_type;
	typedef typename block_type::bid_type bid_type;
	typedef typename ExtIterator_::vector_type::alloc_strategy alloc_strategy;
	typedef stable_ksort_local::bid_sequence<bid_type,alloc_strategy> bucket_bids_type;
	typedef stable_ksort_local::type_key<value_type> type_key_;
	
	first.flush(); // flush container
	
#ifdef STXXL_IO_STATS
	stats *iostats = stats::get_instance ();
	iostats->reset ();
#endif
	
	reset_io_wait_time();
	

	double begin = stxxl_timestamp();
	(void)(begin);
  
  
	unsigned int i=0;
	config * cfg = config::get_instance();
	const unsigned int m = M/block_type::raw_size;
	const unsigned int write_buffers_multiple = 2;
	const unsigned int read_buffers_multiple = 2;
	const unsigned int ndisks = cfg->ndisks();
	const unsigned int nmaxbuckets = m - (write_buffers_multiple + read_buffers_multiple)*ndisks;
	const unsigned int lognbuckets = static_cast<unsigned>(log2(nmaxbuckets));
	const unsigned int nbuckets = 1<<lognbuckets;
	const unsigned int est_bucket_size = div_and_round_up((last-first)/int64(nbuckets),
		int64(block_type::size)); //in blocks

	STXXL_MSG("Elements to sort: " << (last - first))
	STXXL_MSG("Number of buckets has to be reduced from "<<nmaxbuckets<<" to "<< nbuckets)
	const unsigned int nread_buffers = (m - nbuckets)*read_buffers_multiple/(read_buffers_multiple+write_buffers_multiple);
	const unsigned int nwrite_buffers = (m - nbuckets)*write_buffers_multiple/(read_buffers_multiple+write_buffers_multiple);

	STXXL_MSG("Read buffers in distribution phase: "<<nread_buffers)
	STXXL_MSG("Write buffers in distribution phase: "<<nwrite_buffers)

	bucket_bids_type * bucket_bids = new bucket_bids_type[nbuckets](est_bucket_size);
	int64 * bucket_sizes = new int64[nbuckets];
	
	disk_queues::get_instance()->set_priority_op(disk_queue::WRITE);
	
	stable_ksort_local::distribute(
			bucket_bids,
			bucket_sizes,
			nbuckets,
			lognbuckets,
			first,
			last,
			write_buffers_multiple*ndisks,
			read_buffers_multiple*ndisks );
			
	double dist_end = stxxl_timestamp(),end;
  (void)(dist_end);
	#ifdef COUNT_WAIT_TIME
	double io_wait_after_d = stxxl::wait_time_counter;
  (void)(io_wait_after_d);
	#endif

{
	// sort buckets
	unsigned int write_buffers_multiple_bs = 2; 
	unsigned int max_bucket_size_bl = (m - write_buffers_multiple_bs*ndisks)/2; // in number of blocks
	int64 max_bucket_size_rec = int64(max_bucket_size_bl)*block_type::size; // in number of records
	int64 max_bucket_size_act = 0; // actual max bucket size
	// establish output stream
	
	for(i=0;i<nbuckets;i++)
	{
		max_bucket_size_act = STXXL_MAX(bucket_sizes[i],max_bucket_size_act);
		if(bucket_sizes[i] > max_bucket_size_rec)
		{
			STXXL_ERRMSG("Bucket "<<i<<" is too large: "<<bucket_sizes[i]<<
				" records, maximum: "<<max_bucket_size_rec);
			STXXL_ERRMSG("Recursion on buckets is not yet implemented, aborting.");
			abort();
		}
		
	}
	// here we can increase write_buffers_multiple_b knowing max(bucket_sizes[i])
	// ... and decrease max_bucket_size_bl
	const int max_bucket_size_act_bl = div_and_round_up(max_bucket_size_act,block_type::size);
	STXXL_MSG("Reducing required number of required blocks per bucket from "<<
		max_bucket_size_bl<<" to "<<max_bucket_size_act_bl)
	max_bucket_size_rec = max_bucket_size_act;
	max_bucket_size_bl = max_bucket_size_act_bl;
	const unsigned int nwrite_buffers_bs = m - 2*max_bucket_size_bl;
	STXXL_MSG("Write buffers in bucket sorting phase: "<<nwrite_buffers_bs)
	
	typedef buf_ostream<block_type,typename ExtIterator_::bids_container_iterator> buf_ostream_type;
	buf_ostream_type out(first.bid(),nwrite_buffers_bs);
	
	disk_queues::get_instance()->set_priority_op(disk_queue::READ);
	
	if(first.block_offset())
	{
		// has to skip part of the first block
		block_type * block = new block_type;
		request_ptr req;
		req = block->read(*first.bid());
		req->wait();
		
		for(i=0;i<first.block_offset();i++)
		{
			out << block->elem[i];
		}
		delete block;
	}
	block_type * blocks1 = new block_type[max_bucket_size_bl];
	block_type * blocks2 = new block_type[max_bucket_size_bl];
	request_ptr * reqs1 = new request_ptr [max_bucket_size_bl];
	request_ptr * reqs2 = new request_ptr [max_bucket_size_bl];
	type_key_ *refs1 = new type_key_[max_bucket_size_rec];
	type_key_ *refs2 = new type_key_[max_bucket_size_rec];
	
	// submit reading first 2 buckets (Peter's scheme)
	unsigned int nbucket_blocks = div_and_round_up(bucket_sizes[0],block_type::size);
	for(i=0; i<nbucket_blocks; i++)
		reqs1[i] = blocks1[i].read(bucket_bids[0][i]);
	
	nbucket_blocks = div_and_round_up(bucket_sizes[1],block_type::size);
	for(i=0; i<nbucket_blocks; i++)
		reqs2[i] = blocks2[i].read(bucket_bids[1][i]);
	
	key_type offset = 0;
	unsigned log_k1 = static_cast<int>(ceil(log2(max_bucket_size_rec*sizeof(type_key_)/STXXL_L2_SIZE)));
	unsigned k1 = 1 << log_k1;
	int *bucket1 = new int[k1];
	
	const unsigned int shift = sizeof(key_type)*8 - lognbuckets;
	const unsigned int shift1 = shift - log_k1;
	
	for(unsigned k=0;k<nbuckets;k++)
	{
		nbucket_blocks = div_and_round_up(bucket_sizes[k],block_type::size);
		log_k1 = static_cast<unsigned>(ceil(log2(bucket_sizes[k]*sizeof(type_key_)/STXXL_L2_SIZE)));
		k1 = 1 << log_k1;
		std::fill(bucket1,bucket1 + k1,0);

		STXXL_MSG("Classifying bucket "<<k<<" size:"<<bucket_sizes[k]<<
			" blocks:"<<nbucket_blocks<<" log_k1:"<< log_k1)
		// classify first nbucket_blocks-1 blocks, they are full
		type_key_ * ref_ptr = refs1;
		key_type offset1 = offset + (key_type(1)<<key_type(shift)) * key_type(k);
		for(i = 0;i < nbucket_blocks-1;i++)
		{
			reqs1[i]->wait();
			stable_ksort_local::classify_block(blocks1[i].begin(),blocks1[i].end(),ref_ptr,bucket1,offset1,shift1/*,k1*/);
		}
		// last block might be non-full
		const unsigned int last_block_size = bucket_sizes[k] - int64(nbucket_blocks - 1)*block_type::size;
		reqs1[i]->wait();
		
		//STXXL_MSG("block_type::size: "<<block_type::size<<" last_block_size:"<<last_block_size)
		
		classify_block(blocks1[i].begin(),blocks1[i].begin() + last_block_size,ref_ptr,bucket1,offset1,shift1);
	
		exclusive_prefix_sum(bucket1, k1);
		classify(refs1, refs1 + bucket_sizes[k], refs2, bucket1,offset1, shift1);
	
		type_key_ *c = refs2;
		type_key_ *d = refs1;
		for (i = 0; i < k1; i++)
		{
			type_key_ *cEnd = refs2 + bucket1[i];
			type_key_ *dEnd = refs1 + bucket1[i];
			
			const unsigned int log_k2 = static_cast<unsigned>(log2(bucket1[i])) - 1; // adaptive bucket size
			const unsigned int k2 = 1 << log_k2;
			int *bucket2 = new int[k2];
			const unsigned shift2 = shift1 - log_k2;
		
			// STXXL_MSG("Sorting bucket "<<k<<":"<<i)
			l1sort (c, cEnd, d, bucket2, k2,
					offset1 + (key_type(1)<<key_type(shift1)) * key_type(i) , 
					shift2);
			
			// write out all
			for (type_key_ * p = d; p < dEnd; p++)
				out << (*(p->ptr));
			
			delete [] bucket2;
			c = cEnd;
			d = dEnd;
		}
		// submit next read
		const unsigned bucket2submit = k+2;
		if( bucket2submit < nbuckets )
		{
			nbucket_blocks = div_and_round_up(bucket_sizes[bucket2submit],block_type::size);
			for(i=0; i<nbucket_blocks; i++)
				reqs1[i] = blocks1[i].read(bucket_bids[bucket2submit][i]);
		}
		
		std::swap (blocks1, blocks2);
		std::swap (reqs1, reqs2);
	}
	
	delete [] bucket1;
	delete [] refs1;
	delete [] refs2;
	delete [] blocks1;
	delete [] blocks2;
	delete [] reqs1;
	delete [] reqs2;
	delete [] bucket_bids;
	delete [] bucket_sizes;
	
	if(last.block_offset())
	{
		// has to skip part of the first block
		block_type * block = new block_type;
		request_ptr req = block->read(*last.bid());
		req->wait();
		
		for(i=last.block_offset();i<block_type::size;i++)
		{
			out << block->elem[i];
		}
		delete block;
	}
	
	end = stxxl_timestamp();
	
  }
	
	STXXL_VERBOSE ("Elapsed time        : " << end - begin << " s. Distribution time: " << 
	dist_end - begin << " s")
#ifdef STXXL_IO_STATS
	STXXL_VERBOSE ("reads               : " << iostats->get_reads ()) 
	STXXL_VERBOSE ("writes              : " << iostats->get_writes ())
	STXXL_VERBOSE ("read time           : " << iostats->get_read_time () << " s") 
	STXXL_VERBOSE ("write time          : " << iostats->get_write_time () <<" s")
	STXXL_VERBOSE ("parallel read time  : " << iostats->get_pread_time () << " s")
	STXXL_VERBOSE ("parallel write time : " << iostats->get_pwrite_time () << " s")
	STXXL_VERBOSE ("parallel io time    : " << iostats->get_pio_time () << " s")
#endif
#ifdef COUNT_WAIT_TIME
	STXXL_VERBOSE ("Time in I/O wait(ds): " << io_wait_after_d << " s")
	STXXL_VERBOSE ("Time in I/O wait    : " << stxxl::wait_time_counter << " s")
#endif
}

//! \}

__STXXL_END_NAMESPACE

#endif
