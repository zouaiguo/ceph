#!/bin/sh -ex

mkdir -p mnt.admin mnt.foo

./ceph-fuse -n client.admin mnt.admin
mkdir mnt.admin/bar
touch mnt.admin/out mnt.admin/bar/out
echo READ TEST PASSED >> mnt.admin/out
echo READ TEST IN TREE PASSED >>mnt.admin/bar/out

./ceph auth get-or-create client.foo mon 'allow r' mds 'allow r' osd 'allow rwx' >> keyring
./ceph-fuse -n client.foo mnt.foo

cleanup()
{
  rm -rf mnt.admin/bar
  rm mnt.admin/out
  fusermount -u mnt.foo
  fusermount -u mnt.admin
  rm -rf mnt.admin mnt.foo
}

trap cleanup INT TERM EXIT

expect_false()
{
  if "$@"; then return 1;
  else return 0;
  fi
}

# write operations are not allowed
expect_false mkdir mnt.foo/newdir
expect_false touch newfile
expect_false touch mnt.foo/bar/newfile

# read-only operations are allowed
cat mnt.foo/out
cat mnt.foo/bar/out