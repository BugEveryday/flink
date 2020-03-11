package com.flink.item.util

/**
  * 模拟布隆过滤器
  * 主要是生成一个偏移量，用来标识存储的对象
  */
object BloomFilter {
  // 位图的容量
  // Redis位图应该最大为512 * 1024 * 1024 * 8
  val cap = 1 << 29//最多能存2的29次方个数据

  //生成偏移量。用的是HashMap的原理
  // HashMap => hash(key.hashCode) & (length - 1) => index
  // Redis   => crc(key.hashCode)  & (16384 - 1)  => slot
  def offset( s : String, seed:Int ) : Long = {
    var hash = 0

    for ( c <- s ) {
      hash = hash * seed + c
    }
    // 将数据进行散列后计算位图偏移量
    hash & (cap-1)
  }
}
