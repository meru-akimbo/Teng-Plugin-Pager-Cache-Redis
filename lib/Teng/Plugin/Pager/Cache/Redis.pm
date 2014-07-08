package Teng::Plugin::Pager::Cache::Redis;
use 5.008001;
use strict;
use warnings;
use Class::Accessor::Lite rw => [qw/redis_config/];
use Redis;
use Data::Page;

our $VERSION = "0.01";

our @EXPORT = qw/group_key insert_and_update_pager update_with_pager delete_with_pager fetch_with_pager/;

sub group_key {
    my ($self, $table_name, $order_by_key) = @_;
    return sprintf('pager-%s-%s', $table_name, $order_by_key->{order_by_key});
}

sub insert_and_update_pager {
    my ($self, $table_name, $value, $order_by, $prymary_key) = @_;

    my $order_by_key = $order_by->{order_by_key};
    my $row = $self->insert($table_name => $value);
    $self->_redis->excute(
        ZADD => $self->group_key($table_name, $order_by), $value->{$order_by_key}, $prymary_key->{primary_key}
    );

    return $row;
}

sub update_with_pager {
    my ($self, $table_name, $value, $order_by, $prymary_key) = @_;

    my $order_by_key = $order_by->{order_by_key};
    my $row = $self->update($value);
    $self->_redis->excute(
        ZADD => $self->group_key($table_name, $order_by), $value->{$order_by_key}, $prymary_key->{primary_key}
    );

    return $row;
}

sub delete_with_pager {
    my ($self, $order_by) = @_;

    my $order_by_key = $order_by->{order_by_key};

    $self->delete();
    $self->_redis->excute(
        ZREM => $self->group_key($self->{table_name}, $order_by), $self->prymary_keys
    );
}

sub fetch_with_pager {
    my ($self, $table_name, $order_by, $prymary_key, $args) = @_;

    my $order_by_key = $order_by->{order_by_key};

    my $limit  = $args->{row};
    my $offset = ($args->{page} - 1 ) * $limit;

    my @primary_keys
        = $self->_redis->excute(ZRANGE => $self->group_key($table_name, $order_by_key), $offset, ($limit - 1 +$offset) );
    my @rows  = $self->search($table_name, [ $prymary_key => { IN => \@primary_keys } ], { order_by => 'created' });
    my $count = $self->_redis->excute('ZCARD' => $self->group_key($table_name, $order_by_key));

    my $pager = Data::Page->new();
    $pager->entries_per_page($limit);
    $pager->current_page($args->{page});
    $pager->total_entries($count);

    return (\@rows, $pager);
}

sub _redis {
    my ($self,) = @_;
    return Redis->new(%{ $self->redis_config });
}


1;
__END__

=encoding utf-8

=head1 NAME

Teng::Plugin::Pager::Cache::Redis - It's new $module

=head1 SYNOPSIS

    use Teng::Plugin::Pager::Cache::Redis;

=head1 DESCRIPTION

Teng::Plugin::Pager::Cache::Redis is ...

=head1 LICENSE

Copyright (C) meru_akimbo.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

meru_akimbo E<lt>merukatoruayu0@gmail.comE<gt>

=cut

