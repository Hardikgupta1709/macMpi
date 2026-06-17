class Macmpi < Formula
  desc "High-performance, single-node MPI-1.1 subset library for Apple Silicon"
  homepage "https://github.com/Hardikgupta1709/macMpi"
  url "https://github.com/Hardikgupta1709/macMpi/archive/refs/tags/v1.0.0.tar.gz"
  sha256 "dc1e5d071e5592b3ca91a1d0d2c72f682fd2f80387f94e350a94d7ff45ac1ffa"
  license "MIT"

  depends_on "cmake" => :build

  def install
    system "cmake", "-S", ".", "-B", "build", *std_cmake_args
    system "cmake", "--build", "build"
    system "cmake", "--install", "build"
  end

  test do
    system "#{bin}/mpirun", "--version"
  end
end