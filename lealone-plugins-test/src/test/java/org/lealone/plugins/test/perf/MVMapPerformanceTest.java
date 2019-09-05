// package org.lealone.plugins.test.perf;
//
// import org.h2.mvstore.MVStore;
// import org.lealone.plugins.mvstore.MVStorage;
// import org.lealone.storage.type.ObjectDataType;
// import org.lealone.test.perf.StorageMapPerformanceTest;
//
//// 以单元测试的方式运行会比通过main方法运行得出稍微慢一些的测试结果，
//// 这可能是因为单元测试额外启动了一个ReaderThread占用了一些资源
// public class MVMapPerformanceTest extends StorageMapPerformanceTest {
//
// public static void main(String[] args) throws Exception {
// new MVMapPerformanceTest().run();
// }
//
// @Override
// protected void init() {
// // MVStore.Builder builder = new MVStore.Builder();
// MVStore store = MVStore.open(null);
//
// MVStorage mvs = new MVStorage(store, null);
// map = mvs.openMap("MVMapPerformanceTest", new ObjectDataType(), new ObjectDataType(), null);
// }
//
// }
