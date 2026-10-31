/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_ARBITRATOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_ARBITRATOR_H_

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_sort.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
class TokenizeContext;

class ObIKArbitrator
{
public:
  ObIKArbitrator();
  ~ObIKArbitrator();

  int process(TokenizeContext &ctx);

  int output_result(TokenizeContext &ctx);

private:
  int prepare(TokenizeContext &ctx);

  int add_chain(ObIKTokenChain *chain);

  int record_chain_start(int64_t offset);

  int lookup_chain(int64_t current, ObIKTokenChain *&chain);

  int optimize(TokenizeContext &ctx,
               ObIKTokenChain *option,
               ObFTSortList::CellIter iter,
               int64_t fulltext_len,
               ObIKTokenChain *&best);

  int try_add_next_words(ObIKTokenChain *chain,
                         ObFTSortList::CellIter iter,
                         ObIKTokenChain *option,
                         bool need_conflict,
                         ObList<ObFTSortList::CellIter, ObIAllocator> &conflict_stack);

  int remove_conflict(const ObIKToken &token, ObIKTokenChain *option);

  bool keep_single() const { return true; }

private:
  ObArenaAllocator alloc_;
  // The arbitrator belongs to exactly one worker thread for its whole life
  // (it is a thread-local now), so neither the bucket latches nor the spin
  // mutex inside the default node allocator of this map can ever be observed
  // by another thread.  chains_created_/chains_bucket_num_ track the map that
  // is kept across batches instead of rebuilt per batch.
  typedef common::hash::SimpleAllocer<
            common::hash::HashMapTypes<int64_t, ObIKTokenChain *>::AllocType,
            common::hash::NodeNumTraits<
                common::hash::HashMapTypes<int64_t, ObIKTokenChain *>::AllocType>::NODE_NUM,
            common::hash::NoPthreadDefendMode> IKChainNodeAlloc;
  common::hash::ObHashMap<int64_t, ObIKTokenChain *,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<int64_t>,
                          common::hash::equal_to<int64_t>,
                          IKChainNodeAlloc> chains_;
  bool chains_created_;
  int64_t chains_bucket_num_;
  // Offsets recorded by add_chain, i.e. exactly the keys of chains_, kept in
  // an arena backed array and sorted once before output_result walks them.
  int64_t *chain_starts_;
  int64_t chain_start_cnt_;
  int64_t chain_start_cap_;
  int64_t chain_start_pos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIKArbitrator);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_ARBITRATOR_H_
