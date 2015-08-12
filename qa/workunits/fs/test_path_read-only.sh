#!/bin/sh -ex

mkdir -p mnt.admin mnt.foo

sudo ./ceph-fuse -n client.admin mnt.admin
sudo mkdir -p mnt.admin/bar
sudo chmod 777 mnt.admin/bar
sudo touch mnt.admin/bar/out
sudo chmod 777 mnt.admin/bar/out
echo READ TEST PASSED >>mnt.admin/bar/out

./ceph auth get-or-create client.foo mon 'allow r' mds 'allow r' osd 'allow rwx' >> keyring
sudo ./ceph-fuse -n client.foo mnt.foo

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
expect_false mkdir mnt.foo/bar/newdir
expect_false touch mnt.foo/bar/newfile

# read-only operations are allowed
ls mnt.foo/bar
cat mnt.foo/bar/out

echo OK
