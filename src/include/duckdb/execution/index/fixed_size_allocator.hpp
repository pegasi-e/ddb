//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/fixed_size_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/index/fixed_size_buffer.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

namespace duckdb {

//! The FixedSizeAllocator provides pointers to fixed-size memory segments of pre-allocated memory buffers.
//! The pointers are IndexPointers, and the leftmost byte (metadata) must always be zero.
//! It is also possible to directly request a C++ pointer to the underlying segment of an index pointer.
class FixedSizeAllocator {
public:
	// start Anybase changes
	//! A bounded lookup-only cache. Requires exclusive ART access and no allocator mutation.
	//! Its independent pins outlive every returned address and are released before the ART lock.
	class LookupCache {
	public:
		LookupCache() = default;
		~LookupCache() {
			if (owner) {
				D_ASSERT(owner->lookup_cache == this);
				owner->lookup_cache = nullptr;
			}
		}
		LookupCache(const LookupCache &) = delete;
		LookupCache &operator=(const LookupCache &) = delete;
		void Bind(FixedSizeAllocator &allocator) {
			D_ASSERT(!owner && !allocator.lookup_cache);
			if (allocator.buffers.empty()) {
				return;
			}
			idx_t capacity = 1;
			while (capacity < allocator.buffers.size() && capacity < 256) {
				capacity *= 2;
			}
			entries.resize(capacity);
			owner = &allocator;
			owner->lookup_cache = this;
		}
		data_ptr_t Resolve(const idx_t buffer_id, const bool dirty) {
			auto &entry = entries[buffer_id & (entries.size() - 1)];
			if (entry.id != buffer_id) {
				auto it = owner->buffers.find(buffer_id);
				D_ASSERT(it != owner->buffers.end());
				auto &buffer = *it->second;
				entry.pin = buffer.PinForLookup(dirty);
				entry.base = entry.pin.Ptr();
				entry.buffer = &buffer;
				entry.dirty = dirty;
				entry.id = buffer_id;
			} else if (dirty && !entry.dirty) {
				entry.pin = entry.buffer->PinForLookup(true);
				entry.base = entry.pin.Ptr();
				entry.dirty = true;
			}
			return entry.base;
		}
	private:
		struct Entry {
			idx_t id = NumericLimits<idx_t>::Maximum();
			BufferHandle pin;
			data_ptr_t base = nullptr;
			FixedSizeBuffer *buffer = nullptr;
			bool dirty = false;
		};
		FixedSizeAllocator *owner = nullptr;
		unsafe_vector<Entry> entries;
	};
	// end Anybase changes

	//! We can vacuum 10% or more of the total in-memory footprint
	static constexpr uint8_t VACUUM_THRESHOLD = 10;

public:
	//! Construct a new fixed-size allocator
	FixedSizeAllocator(const idx_t segment_size, BlockManager &block_manager,
	                   MemoryTag memory_tag = MemoryTag::ART_INDEX);

	//! Block manager of the database instance
	BlockManager &block_manager;
	//! Buffer manager of the database instance
	BufferManager &buffer_manager;

public:
	//! Get a new IndexPointer to a segment, might cause a new buffer allocation
	IndexPointer New();
	//! Free the segment of the IndexPointer
	void Free(const IndexPointer ptr);

	//! Get a segment handle.
	inline SegmentHandle GetHandle(const IndexPointer ptr) {
		D_ASSERT(ptr.GetOffset() < available_segments_per_buffer);

		auto buffer_it = buffers.find(ptr.GetBufferId());
		D_ASSERT(buffer_it != buffers.end());

		auto offset = ptr.GetOffset() * segment_size + bitmask_offset;
		auto &buffer = *buffer_it->second;
		return SegmentHandle(buffer, offset);
	}

	//! Returns a pointer of type T to a segment. If dirty is false, then T must be a const class.
	//! DEPRECATED. Use segment handles.
	template <class T>
	inline unsafe_optional_ptr<T> Get(const IndexPointer ptr, const bool dirty = true) {
		return (T *)Get(ptr, dirty);
	}

	//! Returns the data_ptr_t to a segment, and sets the dirty flag of the buffer containing that segment.
	//! DEPRECATED. Use segment handles.
	inline data_ptr_t Get(const IndexPointer ptr, const bool dirty = true) {
		D_ASSERT(ptr.GetOffset() < available_segments_per_buffer);
		// start Anybase changes
		if (lookup_cache) {
			return lookup_cache->Resolve(ptr.GetBufferId(), dirty) + ptr.GetOffset() * segment_size + bitmask_offset;
		}
		// end Anybase changes

		auto buffer_it = buffers.find(ptr.GetBufferId());
		D_ASSERT(buffer_it != buffers.end());

		auto offset = ptr.GetOffset() * segment_size + bitmask_offset;
		auto buffer_ptr = buffer_it->second->GetDeprecated(dirty);
		return buffer_ptr + offset;
	}

	//! Has been loaded into a buffer-managed buffer from a persistent, file-backed block.
	inline bool LoadedFromStorage(const IndexPointer ptr) const {
		D_ASSERT(ptr.GetOffset() < available_segments_per_buffer);

		auto buffer_it = buffers.find(ptr.GetBufferId());
		if (buffer_it == buffers.end()) {
			return false;
		}

		return buffer_it->second->InMemory();
	}

	//! Returns a pointer of type T to a segment, or nullptr, if the buffer is not in memory.
	//! DEPRECATED. Use segment handles.
	template <class T>
	inline unsafe_optional_ptr<T> GetIfLoaded(const IndexPointer ptr) {
		return (T *)GetIfLoaded(ptr);
	}

	//! Returns the data_ptr_t to a segment, or nullptr, if the buffer is not in memory.
	//! DEPRECATED. Use segment handles.
	inline data_ptr_t GetIfLoaded(const IndexPointer ptr) {
		D_ASSERT(ptr.GetOffset() < available_segments_per_buffer);
		D_ASSERT(buffers.find(ptr.GetBufferId()) != buffers.end());

		auto &buffer = buffers.find(ptr.GetBufferId())->second;
		if (!buffer->InMemory()) {
			return nullptr;
		}

		auto buffer_ptr = buffer->GetDeprecated();
		auto raw_ptr = buffer_ptr + ptr.GetOffset() * segment_size + bitmask_offset;
		return raw_ptr;
	}

	//! Resets the allocator, e.g., during 'DELETE FROM table'
	void Reset();

	//! Returns the in-memory size in bytes
	idx_t GetInMemorySize() const;
	//! Returns the segment size.
	inline idx_t GetSegmentSize() const {
		return segment_size;
	}
	//! Returns the total segment count.
	inline idx_t GetSegmentCount() const {
		return total_segment_count;
	}

	//! Returns the upper bound of the available buffer IDs, i.e., upper_bound > max_buffer_id
	idx_t GetUpperBoundBufferId() const;
	//! Merge another FixedSizeAllocator into this allocator. Both must have the same segment size
	void Merge(FixedSizeAllocator &other);

	//! Initialize a vacuum operation, and return true, if the allocator needs a vacuum
	bool InitializeVacuum();
	//! Finalize a vacuum operation by freeing all vacuumed buffers
	void FinalizeVacuum();
	//! Returns true, if an IndexPointer qualifies for a vacuum operation, and false otherwise
	inline bool NeedsVacuum(const IndexPointer ptr) const {
		if (vacuum_buffers.find(ptr.GetBufferId()) != vacuum_buffers.end()) {
			return true;
		}
		return false;
	}
	//! Vacuums an IndexPointer
	IndexPointer VacuumPointer(const IndexPointer ptr);

	//! Returns all FixedSizeAllocator information for serialization
	FixedSizeAllocatorInfo GetInfo() const;
	//! Serializes all in-memory buffers
	void SerializeBuffers(PartialBlockManager &partial_block_manager);
	//! Sets the allocation sizes and returns data to serialize each buffer
	vector<IndexBufferInfo> InitSerializationToWAL();
	//! Initialize a fixed-size allocator from allocator storage information
	void Init(const FixedSizeAllocatorInfo &info);
	//! Deserializes all metadata of older storage files
	void Deserialize(MetadataManager &metadata_manager, const BlockPointer &block_pointer);

	//! Returns true, if the allocator does not contain any segments.
	inline bool Empty() {
		return total_segment_count == 0;
	}
	//! Removes empty buffers.
	void RemoveEmptyBuffers();
	//! Verifies that the number of empty buffers does not exceed the empty buffer threshold.
	void VerifyBuffers();

private:
	//! Memory tag of memory that is allocated through the allocator
	MemoryTag memory_tag;
	//! Allocation size of one segment in a buffer
	//! We only need this value to calculate bitmask_count, bitmask_offset, and
	//! available_segments_per_buffer
	idx_t segment_size;

	//! Number of validity_t values in the bitmask
	idx_t bitmask_count;
	//! First starting byte of the payload (segments)
	idx_t bitmask_offset;
	//! Number of possible segment allocations per buffer
	idx_t available_segments_per_buffer;

	//! Total number of allocated segments in all buffers
	//! We can recalculate this by iterating over all buffers
	idx_t total_segment_count;

	//! Buffers containing the segments.
	unordered_map<idx_t, unique_ptr<FixedSizeBuffer>> buffers;
// start Anybase changes
	LookupCache *lookup_cache = nullptr;
// end Anybase changes
	//! Buffers with free space.
	unordered_set<idx_t> buffers_with_free_space;
	//! Caches the next buffer to be filled up.
	//! Unordered sets make no guarantee that begin() returns the same element.
	//! By caching one of the buffers with free space, we get more consistency when filling buffers.
	optional_idx buffer_with_free_space;

	//! Buffers qualifying for a vacuum (helper field to allow for fast NeedsVacuum checks)
	unordered_set<idx_t> vacuum_buffers;

private:
	//! Returns an available buffer id
	idx_t GetAvailableBufferId() const;
	//! Caches the next buffer that we're going to fill.
	void NextBufferWithFreeSpace();
};

} // namespace duckdb
