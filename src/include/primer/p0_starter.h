//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <memory>
#include <utility>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 public:
  // TODO(P0): Add implementation
  Matrix(int r, int c) : rows(r), cols(c) {
    assert(r > 0);
    assert(c > 0);
    linear = new T[r * c]{};
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() {
    delete(linear);
  }
};

template <typename T>
class RowMatrix : public Matrix<T> {
  using Matrix<T>::rows;
  using Matrix<T>::cols;
  using Matrix<T>::linear;

 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    assert(r > 0);
    assert(c > 0);
    data_ = new T*[r]{};
    for (int idx = 0; idx < rows; ++idx) {
      data_[idx] = &linear[idx * cols];
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override {
    assert(i >= 0 && i < rows);
    assert(j >= 0 && j < cols);
    return data_[i][j];
  }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override {
    assert(i >= 0 && i < rows);
    assert(j >= 0 && j < cols);
    data_[i][j] = val;
  }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    memmove(linear, arr, sizeof(T) * rows * cols);
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override {
    delete data_;
  }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>>&& mat1,
                                                   std::unique_ptr<RowMatrix<T>>&& mat2) {
    int r1 = mat1->GetRows();
    int c1 = mat1->GetColumns();
    int r2 = mat2->GetRows();
    int c2 = mat2->GetColumns();
    assert(r1 == r2);
    assert(c1 == c2);
    std::unique_ptr<RowMatrix<T>> res = std::make_unique<RowMatrix<T>>(r1, c1);
    for (int idx1 = 0; idx1 < r1; ++idx1) {
      for (int idx2 = 0; idx2 < c1; ++idx2) {
        T val1 = mat1->GetElem(idx1, idx2);
        T val2 = mat2->GetElem(idx1, idx2);
        res->SetElem(idx1, idx2, val1 + val2);
      }
    }
    return res;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    int r1 = mat1->GetRows();
    int c1 = mat1->GetColumns();
    int r2 = mat2->GetRows();
    int c2 = mat2->GetColumns();
    assert(c1 == r2);
    int dim = c1;
    std::unique_ptr<RowMatrix<T>> res = std::make_unique<RowMatrix<T>>(r1, c2);
    for (int idx1 = 0; idx1 < r1; ++idx1) {
      for (int idx2 = 0; idx2 < c2; ++idx2) {
        T v{};
        for (int idx3 = 0; idx3 < dim; ++idx3) {
          T val1 = mat1->GetElem(idx1, idx3);
          T val2 = mat2->GetElem(idx3, idx2);
          v += val1 * val2;
        }
        res->SetElem(idx1, idx2, v);
      }
    }
    return res;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>>&& matA,
                                                    std::unique_ptr<RowMatrix<T>>&& matB,
                                                    std::unique_ptr<RowMatrix<T>>&& matC) {
    return AddMatrices(MultiplyMatrices(std::move(matA), std::move(matB)), std::move(matC));
  }
};
}  // namespace bustub
